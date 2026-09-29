from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.promo"),
    )
)

result = (
    flattened
    .select("id", "customer_id", "product_id", "qty", "promo")
    .orderBy(F.col("qty").asc_nulls_first())
)
