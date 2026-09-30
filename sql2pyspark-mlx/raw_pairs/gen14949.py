from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("flag_bit8").desc(), "id", "key")
w2 = Window.partitionBy("key", "service").orderBy("flag_bit8", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service", "key").orderBy("status_code_high", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key", "service")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

unpacked = (
    lines
    .filter(F.col("key") == "version")
    .select(
        "id",
        "service",
        "key",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    unpacked
    .filter(F.col("flag_bit8") == 0)
    .withColumn("flag_bit8_bucket", F.ntile(10).over(w))
    .withColumn("first_known_status_code_high", F.first("status_code_high", ignorenulls=True).over(w2))
)

metrics = (
    scored
    .withColumn("running_count_flag_bit8", F.count("flag_bit8").over(w3))
    .withColumn("running_max_flag_bit8", F.max("flag_bit8").over(w3))
    .withColumn("running_min_flag_bit8", F.min("flag_bit8").over(w3))
)

matched_keys = (
    metrics
    .select("service", "id")
    .intersect(
        df_app_logs
        .filter(F.to_date("log_ts") >= "2025-05-22")
        .select("service", "id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics_app_logs = (
    reconciled
    .select(
        "*",
        F.stddev_samp("first_known_status_code_high").over(w4).alias("key_stddev_samp_first_known_status_code_high"),
    )
)

result = (
    metrics_app_logs
    .groupBy("service")
    .agg(
        F.sum(F.when(F.col("service") == "search", F.col("key_stddev_samp_first_known_status_code_high")).otherwise(0)).alias("search_key_stddev_samp_first_known_status_code_high"),
        F.avg("key_stddev_samp_first_known_status_code_high").alias("avg_key_stddev_samp_first_known_status_code_high"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_rows"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
        F.countDistinct("service").alias("distinct_services"),
        F.avg("first_known_status_code_high").alias("avg_first_known_status_code_high"),
    )
    .orderBy("search_rows")
)
