from pyspark.sql import functions as F

flattened = df_carts.select("id", F.inline("items"))

base = (
    flattened
    .filter(F.col("price").between(130, 220))
    .withColumn("promo_title", F.initcap(F.trim("promo")))
    .withColumn("promo_clean", F.coalesce("promo", F.lit("UNKNOWN")))
)

matched_keys = (
    base
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("customer_id").isNotNull())
        .select("id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("product_id") > 4)
    .select(
        "product_id",
        "qty",
        "price",
        "promo",
        "promo_title",
        "promo_clean",
        F.greatest("product_id", "price").alias("greatest_product_id_price"),
    )
)

result = (
    valid
    .select(
        "product_id",
        "price",
        "promo",
        "promo_clean",
        "greatest_product_id_price",
        F.when(F.col("promo").rlike("^(VIP|BUNDLE)$"), "EXTENDED").otherwise("PRIORITY").alias("promo_zone"),
    )
    .orderBy("price")
)
