from pyspark.sql import functions as F

result = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select(
        "id",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)
