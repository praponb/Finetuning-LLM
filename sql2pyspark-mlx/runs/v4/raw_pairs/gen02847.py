from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("num_context", "id")
w2 = Window.partitionBy("service").orderBy("num_context", "id")
w_running = Window.partitionBy("service").orderBy("status_code", "id")

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        "payload_json",
        F.col("context")["version"].alias("version"),
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context")["trace"].alias("trace"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    parsed
    .withColumn("num_context_bucket", F.ntile(10).over(w_part))
    .withColumn("next_num_context", F.lead("num_context", 2, 0).over(w2))
    .withColumn("row_seq", F.row_number().over(w_running))
)
