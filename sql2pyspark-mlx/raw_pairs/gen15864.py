from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").desc(), "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy("session_id", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("attr_key").orderBy("running_max_status_code_int", "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

with_gaps = (
    flattened
    .withColumn("new_group_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        "attr_key",
        "v",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

ranked = (
    islands
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    ranked
    .select(
        "id",
        "host",
        "attr_key",
        "v",
        F.count("status_code").over(w4).alias("running_count_status_code"),
        F.avg("status_code").over(w4).alias("running_avg_status_code"),
        F.max("status_code").over(w4).alias("running_max_status_code"),
    )
)

valid = (
    scored
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("version"))))
    .withColumn("running_max_status_code_minus_running_count_status_code", (F.col("running_max_status_code") - F.col("running_count_status_code")))
    .withColumn("running_max_status_code_int", F.col("running_max_status_code").cast("int"))
)

ranked_app_logs = (
    valid
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked_app_logs
    .select(
        "id",
        "host",
        "attr_key",
        "v",
        "running_count_status_code",
        "running_avg_status_code",
        "running_max_status_code_int",
        "latest_rank",
        F.when(F.col("attr_key").rlike("^(trace|version)$"), "PRIORITY").otherwise("GROUP_B").alias("attr_key_zone"),
    )
    .orderBy("running_count_status_code", "id")
)
