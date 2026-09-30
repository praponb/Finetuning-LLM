from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("attr_key_length", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("running_max_attr_key_length").desc(), "id", "attr_key")
w3 = Window.partitionBy("attr_key").orderBy("running_min_attr_key_length", "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy(F.col("running_max_attr_key_length_bucket").desc(), "id", "attr_key")
w5 = Window.partitionBy("attr_key").orderBy(F.col("running_min_attr_key_length_bucket").desc(), "id", "attr_key")
w6 = Window.partitionBy("attr_key").orderBy("attr_key_length", "id", "attr_key")

exploded = df_carts.select("id", F.explode("attributes").alias("attr_key", "attr_value"))

valid = (
    exploded
    .filter(F.col("attr_key").isNotNull())
    .withColumn("attr_key_length", F.length("attr_key"))
    .withColumn("attr_key_lower", F.lower("attr_key"))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

metrics = (
    valid
    .select(
        "id",
        "attr_key",
        "attr_key_length",
        "attr_key_lower",
        F.min("attr_key_length").over(w).alias("running_min_attr_key_length"),
        F.sum("attr_key_length").over(w).alias("running_sum_attr_key_length"),
        F.max("attr_key_length").over(w).alias("running_max_attr_key_length"),
        F.count("attr_key_length").over(w).alias("running_count_attr_key_length"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.ntile(3).over(w2).alias("running_max_attr_key_length_bucket"),
        F.ntile(4).over(w3).alias("running_min_attr_key_length_bucket"),
    )
)

with_windows = (
    scored
    .filter(F.col("attr_key_length") == 75)
    .select(
        "id",
        "attr_key",
        "attr_key_lower",
        "running_min_attr_key_length",
        "running_max_attr_key_length",
        "running_max_attr_key_length_bucket",
        "running_min_attr_key_length_bucket",
        F.row_number().over(w4).alias("seq_no"),
        F.ntile(3).over(w5).alias("running_min_attr_key_length_bucket_bucket"),
        F.row_number().over(w6).alias("seq_no_2"),
    )
)

base = (
    with_windows
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("weight_kg"))))
    .select(
        "id",
        "attr_key",
        "attr_key_lower",
        "running_min_attr_key_length",
        "running_max_attr_key_length_bucket",
        "running_min_attr_key_length_bucket",
        "seq_no",
        "running_min_attr_key_length_bucket_bucket",
        "seq_no_2",
        F.floor("running_max_attr_key_length_bucket").alias("running_max_attr_key_length_bucket_whole"),
        F.coalesce("attr_key", F.lit("N/A")).alias("resolved_attr_key"),
    )
)

stats = (
    base
    .rollup("attr_key")
    .agg(
        F.percentile_approx("running_min_attr_key_length_bucket", 0.5).alias("running_min_attr_key_length_bucket_p50"),
        F.count(F.when(F.col("attr_key") == "device", 1)).alias("device_rows"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.sum(F.when(F.col("attr_key") == "gift", F.col("seq_no_2")).otherwise(0)).alias("gift_seq_no_2"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys_2"),
        F.sum(F.when(F.col("attr_key") == "weight_kg", F.col("running_max_attr_key_length_bucket_whole"))).alias("weight_kg_total_running_max_attr_key_length_bucket_whole"),
    )
)

result = (
    stats
    .select(
        "attr_key",
        "running_min_attr_key_length_bucket_p50",
        "device_rows",
        "num_attr_keys",
        "gift_seq_no_2",
        "num_attr_keys_2",
    )
    .orderBy(F.col("gift_seq_no_2").desc())
)
