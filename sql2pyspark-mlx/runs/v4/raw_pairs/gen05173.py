from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.date_trunc("WEEK", "shipped_date").alias("week_start"),
        F.when(F.col("cost") >= 105, "HIGH").otherwise("LOW").alias("cost_tier"),
    )
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.size(F.collect_set("status")).alias("num_status_2"),
    )
    .filter(F.col("avg_cost") >= 0)
    .orderBy(F.col("num_status").asc_nulls_last())
)
