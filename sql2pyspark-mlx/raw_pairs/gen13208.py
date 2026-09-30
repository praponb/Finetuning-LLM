from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w2 = Window.partitionBy("key").orderBy(F.col("key_length").desc_nulls_last())
w3 = Window.partitionBy("key").orderBy(F.col("key_length").desc())

flattened = (
    df_carts
    .select("id", "coupon_codes", F.explode("attributes").alias("key", "attr_value"))
)

filtered = (
    flattened
    .filter(F.col("key").isNotNull())
    .withColumn("key_length", F.length("key"))
    .withColumn("key_lower", F.lower("key"))
)

remaining_keys = (
    filtered
    .select("id")
    .exceptAll(
        df_carts
        .filter(F.array_contains("coupon_codes", "VIP"))
        .select("id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "*",
        F.count("key_length").over(w).alias("key_count_key_length"),
        F.percent_rank().over(w2).alias("key_length_percent_rank"),
        F.percent_rank().over(w3).alias("key_length_percent_rank_2"),
    )
)

base = (
    enriched
    .filter(F.col("key_length").isNotNull())
    .select(
        "key",
        "attr_value",
        "key_count_key_length",
        "key_length_percent_rank_2",
        (F.col("key_length") + F.col("key_length_percent_rank_2")).alias("key_length_plus_key_length_percent_rank_2"),
        F.round((F.col("key_length_percent_rank_2") * 1.1), 2).alias("key_length_percent_rank_2_adjusted"),
    )
)

enriched_carts = (
    base
    .filter(F.col("key_count_key_length") > 8)
    .withColumn("key_min_key_count_key_length", F.min("key_count_key_length").over(w))
)

cleaned = (
    enriched_carts
    .filter(F.col("key_length_plus_key_length_percent_rank_2") <= 70)
    .withColumn("adjusted_key_length_plus_key_length_percent_rank_2", F.round((F.col("key_length_plus_key_length_percent_rank_2") * 0.9), 2))
)

result = (
    cleaned
    .select(
        "key_count_key_length",
        "key_length_plus_key_length_percent_rank_2",
        "key_length_percent_rank_2_adjusted",
        "key_min_key_count_key_length",
        "adjusted_key_length_plus_key_length_percent_rank_2",
    )
)
