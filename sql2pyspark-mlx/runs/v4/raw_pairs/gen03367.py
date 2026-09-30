from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("status_code", "id", "key")
w4 = Window.partitionBy("host").orderBy("log_ts", "id", "key")
w5 = Window.partitionBy("host").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

metrics = (
    lines
    .select(
        "*",
        F.sum("status_code").over(w_grp).alias("status_code_sum_2h"),
        F.sum("status_code").over(w2).alias("running_status_code"),
        F.ntile(5).over(w3).alias("status_code_bucket"),
    )
)

flagged = (
    metrics
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w4).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w4) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("session_start_flag").over(w5))

result = (
    sessionized
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "key",
        "value",
        "status_code_sum_2h",
        "status_code_bucket",
        "session_id",
        F.when(F.col("key").rlike("^(trace|region)$"), "CORE").otherwise("GROUP_A").alias("key_zone"),
    )
)
