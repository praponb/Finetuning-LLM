from pyspark.sql import functions as F

lines = df_app_logs.select("id", "status_code", F.explode("context").alias("attr_key", "v"))

result = (
    lines
    .groupBy(
        "attr_key",
        F.when(F.col("status_code") >= 411, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.avg("status_code").alias("avg_status_code"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
    )
)
