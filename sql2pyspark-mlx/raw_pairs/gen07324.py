from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "created_at",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.promo"),
    )
)

result = flattened.select("id", "created_at", "product_id", "qty", "promo")
