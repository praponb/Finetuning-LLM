from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at_year_runnin = Window.orderBy(F.col("closed_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_closed_at_year_ties = Window.orderBy(F.col("closed_at_year").desc())
w_all_by_closed_at_year_prior = Window.orderBy(F.col("closed_at_year").desc(), "id").rowsBetween(-1, -1)
w_all_by_closed_at_year = Window.orderBy(F.col("closed_at_year").desc(), "id")
w_dedup = Window.partitionBy("channel", "agent_id").orderBy(F.col("csat_change").desc(), F.col("id").desc())

prepared_tickets = (
    df_tickets
    .filter((F.col("channel") == "phone") & (F.col("requester_id") > 11))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.year(F.col("closed_at")).alias("closed_at_year"),
        F.concat(F.col("priority"), F.lit("#"), F.col("id").cast("string")).alias("priority_ref"),
    )
)

sequenced = (
    prepared_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        "closed_at_year",
        "priority_ref",
        F.max("csat").over(w_all_by_closed_at_year_runnin).alias("max_csat_so_far"),
        F.dense_rank().over(w_all_by_closed_at_year_ties).alias("closed_at_year_dense_rank"),
        F.round(F.avg("csat").over(w_all_by_closed_at_year_prior), 2).alias("csat_avg_1_prior"),
        (F.col("csat") - F.lag("csat").over(w_all_by_closed_at_year)).alias("csat_change"),
    )
)

enriched = (
    sequenced
    .select(
        "*",
        F.dayofweek(F.col("opened_at")).alias("opened_at_dayofweek"),
        F.round(F.col("csat_avg_1_prior") * 1.5, 2).alias("csat_avg_1_prior_adj"),
    )
)

latest_per_key = (
    enriched
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

totals = (
    latest_per_key
    .groupBy(F.date_trunc("month", "opened_at").alias("opened_at_month"))
    .agg(
        F.percentile_approx("closed_at_year", 0.5).alias("median_closed_at_year"),
        F.count("*").alias("cnt"),
        F.round(F.avg("csat_avg_1_prior_adj"), 2).alias("avg_csat_avg_1_prior_adj"),
        F.min("closed_at").alias("first_closed_at"),
        F.max("opened_at_dayofweek").alias("max_opened_at_dayofweek"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
    )
)

with_activity = (
    totals
    .crossJoin(totals.agg(F.avg("cnt").alias("overall_avg_cnt")))
    .filter(F.col("cnt") > F.col("overall_avg_cnt"))
    .drop("overall_avg_cnt")
)

result = (
    with_activity
    .rollup("median_closed_at_year", "distinct_agent_ids", "cnt")
    .agg(
        F.grouping("median_closed_at_year").alias("is_total_median_closed_at_year"),
        F.min("median_closed_at_year").alias("min_median_closed_at_year"),
        F.min("cnt").alias("min_cnt"),
        F.round(F.sum("avg_csat_avg_1_prior_adj") / F.nullif(F.sum("cnt"), F.lit(0)), 3).alias("avg_csat_avg_1_prior_adj_to_cnt_ratio"),
    )
    .orderBy(
        F.col("median_closed_at_year").asc_nulls_last(),
        F.col("distinct_agent_ids").asc_nulls_last(),
        F.col("cnt").asc_nulls_last(),
    )
)
