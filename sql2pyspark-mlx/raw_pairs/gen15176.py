from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code_high", "id", "attr_key")
w2 = Window.partitionBy("host").orderBy(F.col("status_code_high").desc())
w3 = Window.partitionBy("attr_key", "host").orderBy(F.col("status_code_high_bucket").asc_nulls_first(), "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    flattened
    .select(
        "id",
        "host",
        "attr_key",
        "attr_value",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    parsed
    .select(
        "id",
        "host",
        "attr_key",
        "has_fault",
        F.ntile(10).over(w).alias("status_code_high_bucket"),
        F.cume_dist().over(w2).alias("status_code_high_cume_dist"),
        F.row_number().over(w).alias("row_seq"),
    )
)

deduped = (
    with_windows
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("status_code_high_cume_dist") >= 0.61, "PREMIUM").otherwise("STANDARD").alias("status_code_high_cume_dist_tier"),
        "attr_key",
        F.when(F.col("status_code_high_bucket") >= 6, "HIGH").otherwise("LOW").alias("status_code_high_bucket_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("status_code_high_cume_dist"), 2).alias("avg_status_code_high_cume_dist"),
        F.round(F.sum("has_fault") / F.nullif(F.count(F.when(F.col("attr_key") == "region", 1)), F.lit(0)), 2).alias("has_fault_per_region"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_count"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.sum(F.when(F.col("host") == "h4", F.col("status_code_high_bucket")).otherwise(0)).alias("h4_status_code_high_bucket"),
    )
    .orderBy(F.col("h4_status_code_high_bucket").desc())
)
