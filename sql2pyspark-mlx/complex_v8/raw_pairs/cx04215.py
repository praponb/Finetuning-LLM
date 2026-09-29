from pyspark.sql import functions as F

valid = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) | (F.col("premium") <= 4678.8))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

status_stats = (
    valid
    .groupBy("status")
    .agg(F.avg("premium").alias("avg_premium"))
)

outliers = (
    valid.alias("s")
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
    .groupBy("holder_id", "end_date")
    .pivot("status", ["EXPIRED", "ACTIVE"])
    .agg(F.count("avg_premium"))
    .orderBy("holder_id", "end_date")
)
