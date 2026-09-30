from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("status_code").desc(), "id")
w2 = Window.orderBy(F.col("is_flagged").asc_nulls_last())

unpacked = (
    df_app_logs
    .filter(F.to_date("log_ts") < "2023-07-01")
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "payload_json",
        "context",
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
        F.col("context")["trace"].alias("trace"),
        F.col("context")["region"].alias("region"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.percent_rank().over(w2).alias("is_flagged_percent_rank"),
    )
)
