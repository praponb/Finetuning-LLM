from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "v"),
    )
)

flattened_carts = flattened.select("id", "k", "v", F.explode("coupon_codes").alias("element"))

filtered = (
    flattened_carts
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("element")).alias("element_title"),
        F.coalesce("element", F.lit("NONE")).alias("element_filled"),
    )
)

cleaned = (
    filtered
    .filter(F.col("k") == "utm_source")
    .select(
        "id",
        "k",
        "v",
        "element_title",
        "element_filled",
        F.concat("element", F.lit(" "), "k").alias("element_k"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_clean"),
    )
)

result = (
    cleaned
    .groupBy("k")
    .agg(
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("k") == "device", 1)).alias("device_count"),
    )
)
