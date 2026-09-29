from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("latency_ms").asc_nulls_last(), "id", "attr_key")
w2 = Window.partitionBy("service", "attr_key")
w3 = Window.partitionBy("service").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

base = (
    exploded
    .filter(F.col("attr_key").isNotNull() & (F.col("service") == "billing"))
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "attr_key",
        "value",
        F.upper("service").alias("service_upper"),
        F.trim("service").alias("clean_service"),
    )
)

ranked = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

cleaned = (
    ranked
    .filter(F.col("attr_key").isNotNull())
    .withColumn("latency_ms_str", F.col("latency_ms").cast("string"))
    .withColumn("status_code_whole", F.ceil("status_code"))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("DIRECT")))
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("latency_ms") != 1838)
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "attr_key",
        "value",
        "service_upper",
        "rn",
        "status_code_whole",
        F.length("attr_key").alias("attr_key_length"),
        F.coalesce("attr_key", F.lit("N/A")).alias("resolved_attr_key"),
    )
)

enriched = (
    cleaned_app_logs
    .filter(F.col("attr_key") != "trace")
    .withColumn("service_stddev_samp_status_code_whole", F.stddev_samp("status_code_whole").over(w2))
    .withColumn("running_status_code", F.sum("status_code").over(w3))
)

result = (
    enriched
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "attr_key",
        "value",
        "rn",
        "status_code_whole",
        "attr_key_length",
    )
    .orderBy(F.col("attr_key_length").desc(), "rn")
)
