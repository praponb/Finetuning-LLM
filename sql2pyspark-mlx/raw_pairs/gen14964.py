from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
    )
)

with_gaps = (
    flattened_attrs
    .withColumn("new_group_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "log_ts",
        "level",
        "key",
        "status_code_high",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

with_gaps_2 = (
    islands
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands_app_logs = (
    with_gaps_2
    .select(
        "id",
        "log_ts",
        "level",
        "key",
        "status_code_high",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

result = (
    islands_app_logs
    .groupBy("level")
    .pivot("key", ["trace", "region"])
    .agg(F.max("status_code_high"))
)
