from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("host").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("host", "attr_key").orderBy(F.col("status_code").desc(), "id", "attr_key")
w4 = Window.partitionBy("host").orderBy("latency_ms", "id", "attr_key")
w5 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

flagged = (
    flattened
    .withColumn("is_new_burst", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

base = (
    islands
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("attr_key").isNotNull() & F.hour("log_ts").between(8, 16))
    .withColumn("is_h4", F.when(F.col("host") == "h4", 1).otherwise(0))
)

ranked = (
    base
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked
    .select(
        "*",
        F.lag("is_new_burst", 2).over(w4).alias("prev_is_new_burst"),
        F.max("latency_ms").over(w5).alias("latency_ms_max_30m"),
    )
)

result = (
    enriched
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "attr_key",
        "attr_value",
        "row_num",
        "prev_is_new_burst",
        F.when(F.col("attr_key").rlike("^(region|version)$"), "OTHER_ZONE").when(F.col("attr_key").rlike("^(trace)$"), "GROUP_B").otherwise("GROUP_A").alias("attr_key_zone"),
    )
    .orderBy(F.col("attr_key").desc())
)
