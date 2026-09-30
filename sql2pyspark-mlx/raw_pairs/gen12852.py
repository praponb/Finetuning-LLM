from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("num_context", "id")
w_grp = Window.orderBy(F.col("num_context").desc_nulls_last())

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "payload_json",
        "context",
        F.col("context")["version"].alias("version"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("none")).alias("trace_or_default"),
        F.size("context").alias("num_context"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    flattened_attrs
    .withColumn("next_duration_sec", F.lead("duration_sec", 1).over(w_part))
    .withColumn("num_context_dense_rank", F.dense_rank().over(w_grp))
)
