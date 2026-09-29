from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("host")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("k", "v"),
    )
)

remaining_keys = (
    exploded
    .select("service", "host")
    .intersect(
        df_app_logs
        .filter(F.col("host").isin(["h1", "h4", "h3"]))
        .select("service", "host")
    )
)

verified = (
    exploded
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("latency_ms_sum_30m", F.sum("latency_ms").over(w))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("latency_ms_share") >= 0.77, "LARGE").otherwise("SMALL").alias("latency_ms_share_tier"),
    )
    .agg(
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_count"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
    )
    .filter(F.col("auth_count") > 1)
    .orderBy(F.col("auth_count").desc_nulls_first())
)
