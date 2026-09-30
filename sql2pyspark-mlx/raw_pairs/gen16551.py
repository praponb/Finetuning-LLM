from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("status_code_high", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("k").orderBy(F.col("status_code_high").desc_nulls_last())
w3 = Window.orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("k", "host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("host").orderBy("log_ts", "id", "k")
w6 = Window.partitionBy("host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

unpacked = (
    lines
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "k",
        "attr_value",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    unpacked
    .filter(F.col("host").isin(["h4", "h1"]))
    .withColumn("running_status_code_high", F.sum("status_code_high").over(w))
    .withColumn("status_code_high_dense_rank", F.dense_rank().over(w2))
    .withColumn("first_known_status_code_high", F.first("status_code_high", ignorenulls=True).over(w3))
    .withColumn("running_flag_bit2", F.sum("flag_bit2").over(w_hist))
)

filtered = (
    enriched
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        (F.col("status_code_high") * F.col("running_flag_bit2")).alias("status_code_high_times_running_flag_bit2"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_filled"),
    )
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w5).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w5) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "k",
        "attr_value",
        "status_code_high",
        "running_status_code_high",
        "status_code_high_times_running_flag_bit2",
        "k_filled",
        F.sum("new_group_flag").over(w6).alias("island_id"),
    )
)

result = (
    islands
    .select(
        "id",
        "host",
        "log_ts",
        "k",
        "attr_value",
        "status_code_high",
        "running_status_code_high",
        "status_code_high_times_running_flag_bit2",
    )
    .orderBy(F.col("host").desc())
)
