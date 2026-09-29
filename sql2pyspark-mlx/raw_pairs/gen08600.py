from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("level").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("duration_sec", "id")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "level",
        "status_code",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "status_code",
        "duration_sec",
        "user_id",
        "version_or_default",
        F.max("status_code").over(w).alias("level_max_status_code"),
        F.last("duration_sec", ignorenulls=True).over(w2).alias("last_known_duration_sec"),
        F.ntile(5).over(w3).alias("duration_sec_bucket"),
    )
)
