from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service", "level").orderBy("log_ts", "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

valid = (
    exploded
    .filter(F.col("attr_key").isNotNull() & F.col("attr_key").isin(["trace", "version"]))
    .select(
        "*",
        F.floor("status_code").alias("status_code_whole"),
        F.hour("log_ts").alias("log_ts_hour"),
    )
)

flagged = (
    valid
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = (
    flagged
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "attr_key",
        "status_code_whole",
        "log_ts_hour",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

latest = (
    sessions_app_logs
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.sum(F.when(F.col("host") == "h3", F.col("session_id")).otherwise(0)).alias("h3_session_id"),
        F.percentile_approx("status_code_whole", [0.25, 0.5, 0.75]).alias("status_code_whole_quartiles"),
        F.count("*").alias("cnt"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("host") == "h2", 1)), F.lit(0)), 2).alias("session_id_per_h2"),
    )
)
