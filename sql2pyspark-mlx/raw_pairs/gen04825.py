from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("log_ts"), "host").orderBy("log_ts", "id")
w3 = Window.partitionBy(F.year("log_ts"), "host").orderBy("log_ts", "id")

parsed = (
    df_app_logs
    .filter(F.col("host") == "h4")
    .select(
        "id",
        "host",
        "log_ts",
        "payload_json",
        "context",
        F.col("context")["version"].alias("version"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    parsed
    .filter(~F.col("has_trace"))
    .select(
        "id",
        "host",
        "log_ts",
        "version",
        "user_id",
        F.first("version", ignorenulls=True).over(w).alias("first_known_version"),
        F.first("version", ignorenulls=True).over(w).alias("first_known_version_2"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("event_seq"),
    )
)
