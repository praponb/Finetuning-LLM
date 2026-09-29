from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("status_code", "id")
w_recent = Window.orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_app_logs
    .select(
        "id",
        "level",
        "status_code",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context")["version"].alias("version"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("none")).alias("trace_or_default"),
        F.coalesce(F.col("context").getItem("version"), F.lit("unknown")).alias("version_or_default"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
    )
)

result = (
    unpacked
    .filter(F.col("has_fault") <= 0)
    .select(
        "id",
        "version",
        "version_or_default",
        "has_fault",
        F.sum("has_fault").over(w).alias("cumulative_has_fault"),
        F.lead("has_fault", 1, 0).over(w2).alias("next_has_fault"),
        F.first("has_fault", ignorenulls=True).over(w_recent).alias("first_known_has_fault"),
    )
)
