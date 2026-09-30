from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-06-21 07:00:00").cast("timestamp"))
    .withColumn("started_at_hour", F.hour("started_at"))
    .withColumn("device_lower", F.lower("device"))
    .withColumn("country_filled", F.coalesce("country", F.lit("DIRECT")))
)

gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions = (
    gaps
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "started_at_hour",
        "device_lower",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_sessions
    .rollup(
        "country",
        "device",
        F.when(F.col("started_at_hour") >= 87, "PREMIUM").otherwise("STANDARD").alias("started_at_hour_tier"),
    )
    .agg(
        F.percentile_approx("burst_id", 0.5).alias("burst_id_p50"),
        F.countDistinct("device").alias("distinct_devices"),
    )
    .filter(F.col("distinct_devices") >= 1)
    .orderBy(F.col("device").desc_nulls_first())
)
