from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "value"),
    )
)

flattened_carts = (
    flattened
    .select("id", "attr_key", "value", F.explode_outer("coupon_codes").alias("element"))
)

base = (
    flattened_carts
    .filter(~F.col("attr_key").isin(["weight_kg", "device"]))
    .withColumn("attr_key_title", F.initcap(F.trim("attr_key")))
    .withColumn("element_filled", F.coalesce("element", F.lit("Unknown")))
)

filtered = (
    base
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "attr_key",
        "value",
        "element",
        "attr_key_title",
        "element_filled",
        F.trim("attr_key").alias("clean_attr_key"),
    )
)

kept_keys = (
    filtered
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.array_contains("coupon_codes", "SPRING10"))
        .select("id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupingSets([["attr_key", "element"], ["attr_key"], []], "attr_key", "element")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("element")).alias("element_list"),
        F.count(F.when(F.col("element") == "SPRING10", 1)).alias("spring10_rows"),
        F.count(F.when(F.col("element") == "VIP", 1)).alias("vip_count"),
        F.count(F.when(F.col("attr_key") == "gift", 1)).alias("gift_rows"),
        F.sort_array(F.collect_set("element")).alias("element_list_2"),
        F.grouping("attr_key").alias("grouping_id"),
    )
    .filter(F.col("spring10_rows") >= 1)
)
