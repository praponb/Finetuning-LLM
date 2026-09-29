from pyspark.sql import functions as F

result = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "item"))
    .select(
        "id",
        (F.col("pos") + 1).alias("item_no"),
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.promo"),
    )
)
