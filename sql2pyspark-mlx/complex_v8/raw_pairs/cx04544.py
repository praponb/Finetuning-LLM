from pyspark.sql import functions as F

base = df_tickets.filter(F.col("agent_id") <= 4)

grouped = (
    base
    .groupBy(
        F.date_trunc("month", "opened_at").alias("opened_at_month"),
        F.date_trunc("month", "closed_at").alias("closed_at_month"),
        "csat",
    )
    .agg(
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.round(F.avg("csat"), 2).alias("avg_csat"),
        F.sum("csat").alias("sum_csat_check"),
    )
    .filter(F.col("sum_csat_check") > 3)
    .drop("sum_csat_check")
)

screened = (
    grouped
    .crossJoin(grouped.agg(F.avg("avg_csat").alias("overall_avg_avg_csat")))
    .filter(F.col("avg_csat") > F.col("overall_avg_avg_csat"))
    .drop("overall_avg_avg_csat")
)

screened_2 = (
    screened
    .crossJoin(screened.agg(F.avg("avg_csat").alias("overall_avg_avg_csat")))
    .filter(F.col("avg_csat") > F.col("overall_avg_avg_csat"))
    .drop("overall_avg_avg_csat")
)

result = (
    screened_2
    .filter(F.col("avg_csat") >= 2.6)
    .select(
        "opened_at_month",
        "closed_at_month",
        "csat",
        "channel_list",
        "avg_csat",
    )
    .orderBy(
        F.col("avg_csat").desc(),
        F.col("csat").desc_nulls_first(),
        "opened_at_month",
        "closed_at_month",
    )
    .limit(10)
)
