from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("rec", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        F.col("rec.product_id"),
        F.col("rec.qty"),
        F.col("rec.price"),
        F.col("rec.promo"),
    )
)

result = lines.select("product_id", "qty", "price", "promo")
