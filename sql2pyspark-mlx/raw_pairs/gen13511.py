from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("level") == "DEBUG")
    .select(
        "id",
        "host",
        "level",
        "context",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)
