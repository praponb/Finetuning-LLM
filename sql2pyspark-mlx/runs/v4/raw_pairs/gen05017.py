from pyspark.sql import functions as F

flattened = df_carts.select("id", "coupon_codes", F.explode("attributes").alias("k", "value"))

filtered = (
    flattened
    .filter(F.col("k") == "weight_kg")
    .withColumn("k_lower", F.lower("k"))
    .withColumn("k_title", F.initcap(F.trim("k")))
    .withColumn("k_filled", F.coalesce("k", F.lit("DIRECT")))
)

remaining_keys = (
    filtered
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.array_contains("coupon_codes", "WELCOME5"))
        .select("id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.size("coupon_codes") >= 1)
    .select(
        "k",
        "k_lower",
        "k_title",
        "k_filled",
        F.substring("k", 1, 4).alias("k_prefix"),
    )
)

cleaned = (
    base
    .filter(F.col("k").isNotNull() & F.col("k").isNotNull())
    .select(
        "k",
        "k_lower",
        "k_title",
        "k_filled",
        "k_prefix",
        F.initcap(F.trim("k")).alias("k_title_2"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("resolved_k"),
    )
)

valid = (
    cleaned
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        F.when(F.col("k") == "weight_kg", 1).otherwise(0).alias("is_weight_kg"),
    )
)

result = (
    valid
    .groupBy("k")
    .agg(
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.count(F.when(F.col("k") == "gift", 1)).alias("gift_count"),
        F.size(F.collect_set("k")).alias("num_ks"),
    )
    .filter(F.col("num_ks") > 1)
    .orderBy(F.col("num_ks").desc())
)
