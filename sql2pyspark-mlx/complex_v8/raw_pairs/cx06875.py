from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_min_csat_filled_min_csat_by = Window.partitionBy("min_csat_filled", "min_csat").orderBy("total_csat")
w_first_closed_at_date_year = Window.partitionBy(F.year("first_closed_at_date"))
w_total_csat_rank_by_n_rows_mo = Window.partitionBy("total_csat_rank").orderBy(F.col("n_rows").asc_nulls_last(), "closed_at_date_month").rowsBetween(-3, Window.currentRow)
w_n_rows_by_min_csat = Window.partitionBy("n_rows").orderBy("min_csat", "closed_at_date_month")
w_distinct_prioritys_min_csat = Window.partitionBy("distinct_prioritys", "min_csat_filled")
w_total_csat_rank_by_n_rows_fu = Window.partitionBy("total_csat_rank").orderBy(F.col("n_rows").asc_nulls_last(), "closed_at_date_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_tickets
    .filter((F.col("csat") < 5) | (F.col("channel") == "chat"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
        F.concat(F.col("channel"), F.lit("#"), F.col("csat").cast("string")).alias("channel_ref"),
    )
)

per_group = (
    cleaned
    .groupBy(F.date_trunc("month", "closed_at_date").alias("closed_at_date_month"))
    .agg(
        F.count("*").alias("n_rows"),
        F.min("closed_at_date").alias("first_closed_at_date"),
        F.sum("csat").alias("total_csat"),
        F.countDistinct("priority").alias("distinct_prioritys"),
        F.min("csat").alias("min_csat"),
    )
)

annotated = (
    per_group
    .filter(F.col("min_csat") <= 3)
    .withColumn("min_csat_filled", F.coalesce(F.col("min_csat"), F.lit(0)))
)

sequenced = (
    annotated
    .select(
        "*",
        F.rank().over(w_min_csat_filled_min_csat_by).alias("total_csat_rank"),
        F.max("min_csat_filled").over(w_first_closed_at_date_year).alias("first_closed_at_date_year_max_min_csat_filled"),
        F.min("min_csat_filled").over(w_first_closed_at_date_year).alias("first_closed_at_date_year_min_min_csat_filled"),
    )
)

screened = (
    sequenced
    .crossJoin(sequenced.agg(F.avg("first_closed_at_date_year_min_min_csat_filled").alias("overall_avg_first_closed_at_date_year_min_min_cs")))
    .filter(F.col("first_closed_at_date_year_min_min_csat_filled") > F.col("overall_avg_first_closed_at_date_year_min_min_cs"))
    .drop("overall_avg_first_closed_at_date_year_min_min_cs")
)

first_rows = screened.filter(F.col("total_csat_rank") <= 3)

metrics = (
    first_rows
    .select(
        "closed_at_date_month",
        "first_closed_at_date",
        "total_csat",
        "min_csat",
        "min_csat_filled",
        "first_closed_at_date_year_max_min_csat_filled",
        "first_closed_at_date_year_min_min_csat_filled",
        F.max("total_csat_rank").over(w_total_csat_rank_by_n_rows_mo).alias("total_csat_rank_max_4"),
        F.row_number().over(w_n_rows_by_min_csat).alias("min_csat_seq"),
        F.count(F.lit(1)).over(w_distinct_prioritys_min_csat).alias("rows_in_distinct_prioritys_min_csat_filled"),
        F.nth_value("min_csat", 2).over(w_total_csat_rank_by_n_rows_fu).alias("second_min_csat"),
        F.round(F.col("total_csat_rank") / F.nullif(F.sum("total_csat_rank").over(w_distinct_prioritys_min_csat), F.lit(0)), 4).alias("total_csat_rank_share_of_distinct_prioritys_min_"),
    )
)

result = (
    metrics
    .select(
        "closed_at_date_month",
        "first_closed_at_date",
        "total_csat",
        "min_csat",
        "min_csat_filled",
        "first_closed_at_date_year_max_min_csat_filled",
        "first_closed_at_date_year_min_min_csat_filled",
        "min_csat_seq",
        "rows_in_distinct_prioritys_min_csat_filled",
        "second_min_csat",
        "total_csat_rank_share_of_distinct_prioritys_min_",
        F.coalesce(F.col("total_csat"), F.lit(0)).alias("total_csat_filled"),
        F.abs(F.col("total_csat_rank_max_4") - 21).alias("total_csat_rank_max_4_distance"),
    )
    .orderBy(
        F.col("min_csat").desc(),
        F.col("rows_in_distinct_prioritys_min_csat_filled").asc_nulls_last(),
    )
)
