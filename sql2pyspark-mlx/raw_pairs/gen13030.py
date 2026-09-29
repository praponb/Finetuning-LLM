from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "level").orderBy("log_ts", "id")
w2 = Window.partitionBy("level").orderBy("log_ts", "id")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        "payload_json",
        F.col("context")["version"].alias("version"),
        F.coalesce(F.col("context").getItem("region"), F.lit("unknown")).alias("region_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    flattened_attrs
    .withColumn("next_status_code", F.lead("status_code", 2).over(w))
    .withColumn("next_status_code_2", F.lead("status_code", 1, 0).over(w2))
)
