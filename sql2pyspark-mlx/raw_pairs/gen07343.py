from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

enriched = (
    flattened
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.is_active"))
    .select(
        "a.id",
        "a.customer_id",
        "a.key",
        "a.attr_value",
        F.col("d.country"),
        F.col("d.city"),
    )
)

base = (
    enriched
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "key",
        "attr_value",
        "country",
        "city",
        F.when(F.col("key").isin(["weight_kg", "gift"]), "Priority").otherwise("Other").alias("key_group"),
        F.concat("city", F.lit("/"), "key").alias("city_key"),
    )
)

enriched_carts = (
    base
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.key",
        "src.attr_value",
        "src.country",
        "src.key_group",
        "src.city_key",
        F.col("d.segment"),
    )
)

grouped = (
    enriched_carts
    .groupingSets(
        [["customer_id", "segment"], ["customer_id"]],
        "customer_id",
        "segment",
    )
    .agg(
        F.count(F.when(F.col("key") == "gift", 1)).alias("gift_count"),
        F.count_if(F.col("key") == "utm_source").alias("utm_source_events"),
        F.count(F.when(F.col("key") == "device", 1)).alias("device_count"),
        F.grouping("customer_id").alias("gid"),
    )
)

result = (
    grouped
    .select("gift_count", "gid")
    .orderBy(F.col("gift_count").desc())
)
