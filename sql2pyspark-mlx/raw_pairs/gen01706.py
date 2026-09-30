from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("status_code_high", "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

unpacked = (
    flattened
    .select(
        "id",
        "log_ts",
        "status_code",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

valid = (
    unpacked
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .select(
        "id",
        "log_ts",
        "status_code",
        "attr_key",
        "status_code_high",
        (F.datediff(F.current_date(), "log_ts") / 7).alias("weeks_since_log_ts"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy(
        "attr_key",
        F.when(F.col("status_code_high") >= 2, "PREMIUM").otherwise("STANDARD").alias("status_code_high_tier"),
    )
    .agg(
        F.count(F.when(F.col("attr_key") == "region", 1)).alias("region_count"),
        F.percentile_approx("status_code", 0.95).alias("status_code_p95"),
        F.sum(F.when(F.col("attr_key") == "version", F.col("weeks_since_log_ts")).otherwise(0)).alias("version_weeks_since_log_ts"),
        F.min("status_code").alias("min_status_code"),
        F.count_if(F.col("attr_key") == "region").alias("region_events"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
    )
    .filter(F.col("min_status_code") < 1000000)
)
