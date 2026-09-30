from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("retry_count", "id").rowsBetween(-2, -1)
w_recent_2 = Window.partitionBy(F.date_trunc("YEAR", "log_ts")).orderBy(F.col("retry_count").desc(), "id")
w3 = Window.orderBy("retry_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_app_logs
    .filter(F.to_date("log_ts") >= "2025-01-21")
    .select(
        "id",
        "log_ts",
        "status_code",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.col("context")["trace"].alias("trace"),
    )
)

result = (
    unpacked
    .withColumn("status_code_avg_2_prior", F.avg("status_code").over(w_recent))
    .withColumn("event_seq", F.row_number().over(w_recent_2))
    .withColumn("last_known_retry_count", F.last("retry_count", ignorenulls=True).over(w3))
)
