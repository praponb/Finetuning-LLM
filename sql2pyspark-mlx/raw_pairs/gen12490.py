from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("host").isin(["h3", "h2"]))
    .select(
        "id",
        "host",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    parsed
    .rollup("host")
    .agg(
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_rows"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
    )
)
