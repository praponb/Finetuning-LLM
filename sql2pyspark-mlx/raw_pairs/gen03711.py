from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("host").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .filter(F.col("status_code") > 392)
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "payload_json",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "host",
        "duration_sec",
        F.min("status_code").over(w_recent).alias("running_min_status_code"),
        F.max("status_code").over(w_recent).alias("running_max_status_code"),
        F.count("status_code").over(w_recent).alias("running_count_status_code"),
    )
)
