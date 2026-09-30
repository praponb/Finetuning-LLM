from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("k_length", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("k").orderBy("k_length", "id", "k")
w3 = Window.orderBy(F.col("k_length").desc(), "id", "k")
w4 = Window.partitionBy("k")

flattened = df_carts.select("id", F.explode("attributes").alias("k", "v"))

valid = (
    flattened
    .filter(F.col("k").isNotNull() & F.col("k").isin(["device", "utm_source"]))
    .withColumn("k_length", F.length("k"))
    .withColumn("k_upper", F.upper("k"))
    .withColumn("k_filled", F.coalesce("k", F.lit("UNKNOWN")))
)

with_windows = (
    valid
    .withColumn("cumulative_k_length", F.sum("k_length").over(w))
    .withColumn("k_length_bucket", F.ntile(4).over(w2))
    .withColumn("k_length_bucket_2", F.ntile(5).over(w3))
    .withColumn("k_stddev_samp_k_length", F.stddev_samp("k_length").over(w4))
)

matched_keys = (
    with_windows
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.col("customer_id").isin([19, 23, 29]))
        .select("id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("k").isNotNull())
    .select(
        "k",
        "v",
        "k_upper",
        "k_filled",
        "cumulative_k_length",
        "k_length_bucket_2",
        "k_stddev_samp_k_length",
        (F.col("k_length_bucket_2") / F.nullif("cumulative_k_length", F.lit(0))).alias("k_length_bucket_2_per_cumulative_k_length"),
    )
)

result = (
    cleaned
    .groupBy(
        "k",
        F.when(F.col("k_stddev_samp_k_length") >= 81, "PREMIUM").otherwise("STANDARD").alias("k_stddev_samp_k_length_tier"),
    )
    .agg(
        F.round(F.sum("k_stddev_samp_k_length") / F.nullif(F.count(F.when(F.col("k") == "utm_source", 1)), F.lit(0)), 2).alias("k_stddev_samp_k_length_per_utm_source"),
        F.max("k_length_bucket_2").alias("max_k_length_bucket_2"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
    )
    .filter(F.col("max_k_length_bucket_2") > 0)
)
