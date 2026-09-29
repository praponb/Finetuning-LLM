from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("flag_bit1_scaled").desc(), "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy("flag_bit1", "id", "attr_key")
w3 = Window.orderBy(F.col("status_code_high_per_status_code").desc(), "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

flattened_attrs = (
    flattened
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "attr_key",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    flattened_attrs
    .filter(F.col("attr_key").isNotNull() & F.col("attr_key").isin(["trace", "region"]))
    .select(
        "id",
        "service",
        "latency_ms",
        "attr_key",
        "flag_bit1",
        F.round((F.col("flag_bit1") * 0.9), 2).alias("flag_bit1_scaled"),
        F.round((F.col("status_code_high") / F.col("status_code")), 2).alias("status_code_high_per_status_code"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "latency_ms",
        "attr_key",
        "flag_bit1",
        "status_code_high_per_status_code",
    )
)

enriched = (
    ranked
    .select(
        "id",
        "attr_key",
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(4).over(w3).alias("status_code_high_per_status_code_bucket"),
    )
)

result = (
    enriched
    .withColumn("seq_no_tier", F.when(F.col("seq_no") >= 9, "HIGH").otherwise("LOW"))
    .withColumn("status_code_high_per_status_code_bucket_tier", F.when(F.col("status_code_high_per_status_code_bucket") >= 9, "PREMIUM").otherwise("STANDARD"))
    .rollup("seq_no_tier", "seq_no", "status_code_high_per_status_code_bucket_tier")
    .agg(
        F.max("status_code_high_per_status_code_bucket").alias("max_status_code_high_per_status_code_bucket"),
        F.percentile_approx("status_code_high_per_status_code_bucket", [0.25, 0.5, 0.75]).alias("status_code_high_per_status_code_bucket_quartiles"),
        F.round(F.sum("status_code_high_per_status_code_bucket") / F.nullif(F.count(F.when(F.col("attr_key") == "version", 1)), F.lit(0)), 2).alias("status_code_high_per_status_code_bucket_per_version"),
        F.count(F.when(F.col("attr_key") == "region", 1)).alias("region_count"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.grouping("seq_no_tier").alias("grouping_id"),
    )
    .orderBy(F.col("max_status_code_high_per_status_code_bucket").desc())
)
