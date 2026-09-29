from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("latency_ms", "id", "key")
w2 = Window.partitionBy("key")
w3 = Window.partitionBy("host").orderBy("latency_ms", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

filtered = (
    flattened
    .filter(F.col("level") == "ERROR")
    .select(
        "*",
        F.concat_ws("/", "host", "level").alias("host_level_key"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_clean"),
    )
)

valid = (
    filtered
    .filter(~F.col("level").isin(["ERROR", "INFO", "WARN"]))
    .select(
        "id",
        "host",
        "latency_ms",
        "key",
        "key_clean",
        F.concat_ws("-", "level", "host").alias("level_host_key"),
        F.coalesce("key", F.lit("NONE")).alias("key_clean_2"),
    )
)

scored = (
    valid
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("key_max_latency_ms", F.max("latency_ms").over(w2))
    .withColumn("next_latency_ms", F.lead("latency_ms", 1).over(w3))
)

valid_app_logs = (
    scored
    .filter(F.col("next_latency_ms").between(1126, 2274))
    .select(
        "id",
        "host",
        "key",
        "key_clean",
        "level_host_key",
        "row_seq",
        "key_max_latency_ms",
        "next_latency_ms",
        F.concat_ws(",", "host", "key").alias("host_key_key"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("resolved_key"),
    )
)

result = (
    valid_app_logs
    .select(
        "id",
        "key",
        "level_host_key",
        "row_seq",
        "key_max_latency_ms",
        "next_latency_ms",
        F.when(F.col("key").rlike("^(trace|version)$"), "GROUP_A").when(F.col("key").rlike("^(region)$"), "CORE").otherwise("EXTENDED").alias("key_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
