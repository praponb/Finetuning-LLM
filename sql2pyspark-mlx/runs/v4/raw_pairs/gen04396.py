from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("num_context", "id")

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "payload_json",
        "context",
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
    )
)

result = (
    parsed
    .withColumn("next_num_context", F.lead("num_context", 2).over(w))
    .withColumn("num_context_bucket", F.ntile(5).over(w))
)
