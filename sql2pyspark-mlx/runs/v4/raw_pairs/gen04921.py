from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("attr_key").orderBy(F.col("latency_ms").desc_nulls_first())
w2 = Window.orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "id",
        "latency_ms",
        "attr_key",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

cleaned = (
    flattened_attrs
    .filter(F.col("attr_key").isNotNull() & (F.col("status_code_high") != 1))
    .select("id", "latency_ms", "attr_key", F.upper("attr_key").alias("attr_key_upper"))
)

scored = (
    cleaned
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w_recent))
    .withColumn("cumulative_latency_ms", F.sum("latency_ms").over(w2))
)

result = (
    scored
    .select(
        "id",
        "latency_ms",
        "attr_key",
        "latency_ms_cume_dist",
        F.when(F.col("attr_key").rlike("^(trace|version)$"), "GROUP_B").when(F.col("attr_key").rlike("^(region)$"), "EXTENDED").otherwise("PRIORITY").alias("attr_key_zone"),
    )
    .orderBy(F.col("latency_ms").desc_nulls_last(), "id")
)
