from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("is_weight_kg").desc_nulls_last())
w3 = Window.partitionBy("key", "customer_id")
w4 = Window.partitionBy("key").orderBy(F.col("is_weight_kg").desc_nulls_last())

lines = (
    df_carts
    .select("id", "customer_id", F.explode("attributes").alias("key", "attr_value"))
)

filtered = (
    lines
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.upper("key").alias("key_upper"),
        F.when(F.col("key") == "weight_kg", 1).otherwise(0).alias("is_weight_kg"),
    )
)

kept_keys = (
    filtered
    .select("id", "customer_id")
    .subtract(
        df_carts
        .filter(F.array_contains("coupon_codes", "SPRING10"))
        .select("id", "customer_id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "id",
        "customer_id",
        "key",
        "attr_value",
        "key_upper",
        "is_weight_kg",
        F.avg("is_weight_kg").over(w).alias("customer_id_avg_is_weight_kg"),
        F.cume_dist().over(w2).alias("is_weight_kg_cume_dist"),
        F.min("is_weight_kg").over(w).alias("customer_id_min_is_weight_kg"),
        F.count("is_weight_kg").over(w3).alias("key_count_is_weight_kg"),
        F.percent_rank().over(w4).alias("is_weight_kg_percent_rank"),
    )
)

summary = (
    enriched
    .groupBy(
        F.when(F.col("customer_id_min_is_weight_kg") >= 64, "PREMIUM").otherwise("STANDARD").alias("customer_id_min_is_weight_kg_tier"),
    )
    .agg(
        F.sum(F.when(F.col("key") == "device", F.col("customer_id_avg_is_weight_kg")).otherwise(0)).alias("device_customer_id_avg_is_weight_kg"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
)

result = (
    summary
    .select(
        "customer_id_min_is_weight_kg_tier",
        "device_customer_id_avg_is_weight_kg",
        "distinct_customer_ids",
    )
)
