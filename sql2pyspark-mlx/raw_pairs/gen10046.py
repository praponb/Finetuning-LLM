from pyspark.sql import functions as F

result = (
    df_carts
    .withColumn("entry", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        F.col("entry.product_id"),
        F.col("entry.qty"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)
