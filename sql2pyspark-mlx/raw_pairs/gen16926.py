from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("rec", F.explode("items"))
    .select("id", "customer_id", F.col("rec.product_id"), F.col("rec.price"))
)

cleaned = (
    flattened
    .filter(F.col("price") >= 90)
    .withColumn("product_id_band", F.when(F.col("product_id") >= 10, "High").when(F.col("product_id") >= 3, "Medium").otherwise("Low"))
    .withColumn("adjusted_product_id", F.round((F.col("product_id") * 0.85), 2))
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.country"))
)

with_dims_carts = (
    with_dims
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.customer_id",
        "src.product_id",
        "src.adjusted_product_id",
        "src.country",
        F.col("c.signup_date"),
        F.col("c.country").alias("customer_country"),
        F.col("c.segment"),
    )
)

result = (
    with_dims_carts
    .select(
        "customer_id",
        "product_id",
        "adjusted_product_id",
        "country",
        "signup_date",
        "customer_country",
        "segment",
    )
    .orderBy(F.col("customer_id").desc(), "product_id")
)
