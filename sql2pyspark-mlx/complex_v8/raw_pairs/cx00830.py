from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("priority_or_unknown").orderBy(F.col("closed_at").desc(), F.col("id").desc())
w_all_by_row_count = Window.orderBy(F.col("row_count").asc_nulls_last(), "closed_at_month_month", "closed_at_year", "agent_tier")
w_all_by_row_count_full = Window.orderBy(F.col("row_count").asc_nulls_last(), "closed_at_month_month", "closed_at_year", "agent_tier").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_row_count_median_csat = Window.partitionBy("row_count", "median_csat")

base_tickets = (
    df_tickets
    .filter(F.col("closed_at") < F.lit("2025-02-14 22:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        F.date_trunc("month", F.col("closed_at")).alias("closed_at_month"),
        F.date_format(F.col("opened_at"), "MMM yyyy").alias("opened_at_label"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

joined = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.lower(F.col("su.agent_name")).like("%on%"))
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.closed_at_month",
        "s.priority_or_unknown",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

latest = (
    joined
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

per_group = (
    latest
    .groupBy(
        F.date_trunc("month", "closed_at_month").alias("closed_at_month_month"),
        F.year("closed_at").alias("closed_at_year"),
        "agent_tier",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("csat", 0.5).alias("median_csat"),
    )
)

windowed = (
    per_group
    .select(
        "*",
        F.dense_rank().over(w_all_by_row_count).alias("row_count_dense_rank"),
        F.nth_value("closed_at_year", 2).over(w_all_by_row_count_full).alias("second_closed_at_year"),
        F.round(F.col("closed_at_year") / F.nullif(F.sum("closed_at_year").over(w_row_count_median_csat), F.lit(0)), 4).alias("closed_at_year_share_of_row_count_median_csat"),
        F.round(F.avg("median_csat").over(w_row_count_median_csat), 2).alias("row_count_median_csat_avg_median_csat"),
        F.round(F.col("row_count") / F.nullif(F.sum("row_count").over(w_row_count_median_csat), F.lit(0)), 4).alias("row_count_share_of_row_count_median_csat"),
    )
)

result = (
    windowed
    .select(
        "closed_at_month_month",
        "row_count",
        "row_count_dense_rank",
        "second_closed_at_year",
        "closed_at_year_share_of_row_count_median_csat",
        "row_count_median_csat_avg_median_csat",
        "row_count_share_of_row_count_median_csat",
        F.date_trunc("month", F.col("closed_at_month_month")).alias("closed_at_month_month_month"),
        F.date_format(F.col("closed_at_month_month"), "yyyy-MM-dd").alias("closed_at_month_month_label"),
    )
    .orderBy(F.col("row_count_median_csat_avg_median_csat").desc_nulls_first())
)
