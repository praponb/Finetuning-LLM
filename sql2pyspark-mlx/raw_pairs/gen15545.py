from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "level")
w2 = Window.partitionBy("level", "host")
w3 = Window.partitionBy("level")

exploded = (
    df_app_logs
    .select("id", "host", "level", "latency_ms", F.explode("context").alias("k", "v"))
)

matched_keys = (
    exploded
    .select("id", "level")
    .subtract(
        df_app_logs
        .filter(~F.col("level").isin(["INFO", "DEBUG"]))
        .select("id", "level")
    )
)

reconciled = (
    exploded
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

kept_keys = (
    reconciled
    .select("host")
    .exceptAll(
        df_app_logs
        .filter(F.col("status_code") < 330)
        .select("host")
    )
)

reconciled_app_logs = (
    reconciled
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

scored = (
    reconciled_app_logs
    .filter(F.col("latency_ms") != 2060)
    .withColumn("k_avg_latency_ms", F.avg("latency_ms").over(w))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4))
    .withColumn("level_avg_latency_ms", F.avg("latency_ms").over(w3))
)

result = (
    scored
    .select("host", "latency_ms", "k", "v", "latency_ms_share")
    .orderBy(F.col("latency_ms").desc_nulls_first(), F.col("latency_ms_share").desc())
)
