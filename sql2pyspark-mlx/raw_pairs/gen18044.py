from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("level").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level", "service").orderBy(F.col("log_ts").desc(), "id")

unpacked = (
    df_app_logs
    .filter(F.col("level").isin(["ERROR", "WARN", "DEBUG"]))
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "context",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.col("context").getItem("region").alias("region"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.first("region", ignorenulls=True).over(w_running).alias("first_known_region"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
