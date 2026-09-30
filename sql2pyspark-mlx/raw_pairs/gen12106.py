from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("status_code_high", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("key").orderBy("has_fault", "id", "key")
w3 = Window.partitionBy("key").orderBy(F.col("status_code_high").desc_nulls_last())
w4 = Window.orderBy("has_fault", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("key").orderBy(F.col("status_code_high").desc(), "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

flattened_attrs = (
    lines
    .filter(F.col("key").isin(["version", "trace"]))
    .select(
        "id",
        "key",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    flattened_attrs
    .select(
        "id",
        "key",
        "status_code_high",
        F.first("status_code_high", ignorenulls=True).over(w).alias("first_known_status_code_high"),
        F.ntile(4).over(w2).alias("has_fault_bucket"),
        F.cume_dist().over(w3).alias("status_code_high_cume_dist"),
        F.last("has_fault", ignorenulls=True).over(w4).alias("last_known_has_fault"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

filtered = (
    scored
    .filter(F.col("seq_no") < 6)
    .select(
        "id",
        "key",
        "first_known_status_code_high",
        "has_fault_bucket",
        F.round((F.col("has_fault_bucket") * 100), 2).alias("has_fault_bucket_adjusted"),
        F.coalesce("key", F.lit("NONE")).alias("key_filled"),
    )
)

grouped = (
    filtered
    .withColumn("first_known_status_code_high_tier", F.when(F.col("first_known_status_code_high") >= 2, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["first_known_status_code_high_tier", "key"], ["first_known_status_code_high_tier"], []],
        "first_known_status_code_high_tier",
        "key",
    )
    .agg(
        F.max(F.when(F.col("key") == "region", F.col("has_fault_bucket"))).alias("region_max_has_fault_bucket"),
        F.percentile_approx("has_fault_bucket", 0.5).alias("has_fault_bucket_p50"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    grouped
    .select("has_fault_bucket_p50", "grouping_id")
    .orderBy(F.col("grouping_id").asc_nulls_first())
)
