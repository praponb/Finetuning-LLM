from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) | (F.col("start_date") >= F.lit("2024-05-07").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

status_stats = (
    cleaned
    .groupBy("status")
    .agg(F.avg("premium").alias("avg_premium"))
)

outliers = (
    cleaned.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

result = (
    outliers
    .groupBy("start_date")
    .pivot("status", ["CANCELLED", "EXPIRED", "ACTIVE"])
    .agg(F.sum("premium_minus_avg"))
)
