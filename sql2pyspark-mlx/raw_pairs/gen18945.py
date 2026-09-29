from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("host").orderBy("num_context", "id").rowsBetween(-2, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .filter(F.col("host") == "h3")
    .select(
        "id",
        "host",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.coalesce(F.col("context").getItem("region"), F.lit("unknown")).alias("region_or_default"),
        F.size("context").alias("num_context"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("num_context") <= 67)
    .select(
        "id",
        "duration_text",
        F.min("num_context").over(w_grp).alias("last3_min_num_context"),
        F.max("num_context").over(w_grp).alias("last3_max_num_context"),
        F.sum("num_context").over(w_grp).alias("last3_sum_num_context"),
    )
)
