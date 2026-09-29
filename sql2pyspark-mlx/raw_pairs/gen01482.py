from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("attr_key_length").asc_nulls_first())
w2 = Window.partitionBy("attr_key").orderBy(F.col("attr_key_length_dense_rank").desc_nulls_last())
w_recent = Window.partitionBy("attr_key")
w_recent_2 = Window.partitionBy("attr_key").orderBy(F.col("attr_key_length_dense_rank").asc_nulls_first())
w_hist = Window.orderBy(F.col("attr_key_length_dense_rank").desc_nulls_first())

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

lines_carts = lines.select("id", "attr_key", "v", F.explode_outer("coupon_codes").alias("code"))

cleaned = (
    lines_carts
    .filter(F.col("attr_key") != "utm_source")
    .withColumn("attr_key_length", F.length("attr_key"))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

with_windows = (
    cleaned
    .select(
        "id",
        "attr_key",
        "v",
        F.dense_rank().over(w).alias("attr_key_length_dense_rank"),
    )
)

base = (
    with_windows
    .filter(F.col("attr_key") == "utm_source")
    .withColumn("attr_key_length_dense_rank_tier", F.when(F.col("attr_key_length_dense_rank") >= 7, "High").when(F.col("attr_key_length_dense_rank") >= 3, "Medium").otherwise("Low"))
    .withColumn("attr_key_length_dense_rank_filled", F.coalesce("attr_key_length_dense_rank", F.lit(0)))
)

cleaned_carts = (
    base
    .filter(F.col("attr_key").isNotNull())
    .withColumn("attr_key_length_dense_rank_filled_tier", F.when(F.col("attr_key_length_dense_rank_filled") >= 90, "High").when(F.col("attr_key_length_dense_rank_filled") >= 19, "Medium").otherwise("Low"))
    .withColumn("attr_key_length_dense_rank_per_attr_key_length_dense_rank_filled", (F.col("attr_key_length_dense_rank") / F.nullif("attr_key_length_dense_rank_filled", F.lit(0))))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("DIRECT")))
)

scored = (
    cleaned_carts
    .select(
        "id",
        "v",
        "attr_key_length_dense_rank_tier",
        "attr_key_length_dense_rank_per_attr_key_length_dense_rank_filled",
        "resolved_attr_key",
        F.rank().over(w2).alias("attr_key_length_dense_rank_rank"),
        F.count("attr_key_length_dense_rank_per_attr_key_length_dense_rank_filled").over(w_recent).alias("attr_key_count_attr_key_length_dense_rank_per_attr_key_length_dense_rank_filled"),
        F.rank().over(w_recent_2).alias("attr_key_length_dense_rank_rank_2"),
        F.min("attr_key_length_dense_rank_filled").over(w_recent).alias("attr_key_min_attr_key_length_dense_rank_filled"),
        F.cume_dist().over(w_hist).alias("attr_key_length_dense_rank_cume_dist"),
    )
)

report = (
    scored
    .select(
        "id",
        "v",
        "attr_key_length_dense_rank_tier",
        "attr_key_length_dense_rank_per_attr_key_length_dense_rank_filled",
        "resolved_attr_key",
        "attr_key_length_dense_rank_rank_2",
        "attr_key_min_attr_key_length_dense_rank_filled",
        "attr_key_length_dense_rank_cume_dist",
    )
)

result = (
    report
    .select(
        "id",
        "v",
        "attr_key_length_dense_rank_tier",
        "attr_key_length_dense_rank_per_attr_key_length_dense_rank_filled",
        "resolved_attr_key",
        "attr_key_length_dense_rank_cume_dist",
    )
)
