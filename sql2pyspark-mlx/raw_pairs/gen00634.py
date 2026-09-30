from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

exploded = (
    flattened
    .select(
        "created_at",
        "qty",
        "price",
        "promo",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

filtered = (
    exploded
    .filter(F.col("promo").isNotNull() & (F.col("promo") == "VIP"))
    .select(
        "qty",
        "element",
        F.round("qty", 1).alias("qty_rounded"),
        F.when(F.col("qty") >= 5, "High").when(F.col("qty") >= 2, "Medium").otherwise("Low").alias("qty_tier"),
    )
)

cleaned = (
    filtered
    .filter(F.col("element") == "VIP")
    .withColumn("element_title", F.initcap(F.trim("element")))
)

result = cleaned.select("qty", "element", "qty_rounded", "qty_tier", "element_title")
