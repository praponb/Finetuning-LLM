from pyspark.sql import functions as F

base = (
    df_sessions
    .filter((F.col("browser").eqNullSafe(F.lit("Firefox"))) & F.col("country").isNotNull())
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        F.when(F.col("country") == "TH", 1).otherwise(0).alias("is_th"),
        F.coalesce("browser", F.lit("OTHER")).alias("resolved_browser"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("is_th") >= 73, "LARGE").otherwise("SMALL").alias("is_th_tier"),
    )
    .agg(
        F.sum(F.when(F.col("device") == "mobile", F.col("is_th")).otherwise(0)).alias("mobile_is_th"),
        F.min("started_at").alias("first_started_at"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
        F.round(F.avg("is_th"), 2).alias("avg_is_th"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
