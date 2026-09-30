from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
    )
)

exploded = (
    flattened
    .select(
        "customer_id",
        "product_id",
        "price",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

matched_keys = (
    exploded
    .select("customer_id")
    .intersectAll(
        df_carts
        .filter(F.col("customer_id").isin([11, 19]))
        .select("customer_id")
    )
)

kept_rows = (
    exploded
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(~F.col("value").eqNullSafe(F.lit("BUNDLE")))
    .select(
        "*",
        F.round((F.col("price") / F.col("product_id")), 2).alias("price_per_product_id"),
    )
)

cleaned = (
    filtered
    .filter(F.col("price_per_product_id") >= 75)
    .select(
        "customer_id",
        "product_id",
        "price",
        "value",
        (F.col("product_id") - F.col("price_per_product_id")).alias("product_id_minus_price_per_product_id"),
        F.round((F.col("price_per_product_id") / F.col("product_id")), 2).alias("price_per_product_id_per_product_id"),
    )
)

enriched = (
    cleaned
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.city").isin(["Tokyo", "Bangkok", "Singapore"]))
    .select(
        "src.customer_id",
        "src.price",
        "src.price_per_product_id_per_product_id",
        F.col("b.city"),
        F.col("b.age"),
        F.col("b.email"),
    )
)

joined = (
    enriched
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.country"), F.col("c.credit_limit"))
)

result = (
    joined
    .groupBy("country", "customer_id")
    .pivot("city", ["Chiang Mai", "Singapore"])
    .agg(F.sum("price").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "country",
        "customer_id",
        F.col("Chiang Mai_amount").alias("chiang_mai_amount"),
        F.col("Chiang Mai_n").alias("chiang_mai_n"),
        F.col("Singapore_amount").alias("singapore_amount"),
        F.col("Singapore_n").alias("singapore_n"),
    )
)
