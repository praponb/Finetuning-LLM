from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "payload_json",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
        F.col("context")["region"].alias("region"),
        F.col("context")["version"].alias("version"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("status_code") >= 472, "LARGE").otherwise("SMALL").alias("status_code_tier"),
    )
    .agg(
        F.sum(F.when(F.col("level") == "INFO", F.col("is_flagged")).otherwise(0)).alias("info_is_flagged"),
        F.count_if(F.col("host") == "h2").alias("h2_events"),
        F.size(F.collect_set("host")).alias("num_hosts"),
    )
    .filter(F.col("h2_events") < 1000)
)
