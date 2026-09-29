from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("level", "attr_key").orderBy(F.col("latency_ms").asc_nulls_first())
w3 = Window.partitionBy("host")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

scored = (
    flattened
    .filter(F.col("status_code").between(398, 414))
    .withColumn("latency_ms_sum_1h", F.sum("latency_ms").over(w))
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w2))
    .withColumn("host_count_latency_ms", F.count("latency_ms").over(w3))
)

cleaned = (
    scored
    .filter((F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 07:00:00"))) & (F.col("log_ts") < "2025-05-05"))
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        "latency_ms_sum_1h",
        "latency_ms_cume_dist",
        "host_count_latency_ms",
        F.length("attr_key").alias("attr_key_length"),
    )
)

remaining_keys = (
    cleaned
    .select("id", "level")
    .intersectAll(
        df_app_logs
        .filter(F.col("log_ts") >= "2025-03-01")
        .select("id", "level")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("host")
    .pivot("attr_key", ["version", "trace"])
    .agg(F.avg("latency_ms_sum_1h").alias("amount"))
)
