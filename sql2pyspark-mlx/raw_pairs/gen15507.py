from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code").desc(), "id")
w2 = Window.orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("service", "level").orderBy("status_code", "id").rowsBetween(-5, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.col("context")["region"].alias("region"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "host",
        "status_code",
        "has_trace",
        "region",
        "duration_sec",
        "tier",
        F.ntile(10).over(w).alias("status_code_bucket"),
        F.sum("duration_sec").over(w2).alias("running_duration_sec"),
        F.avg("status_code").over(w_hist).alias("status_code_avg_5"),
    )
)
