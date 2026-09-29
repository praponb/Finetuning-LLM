from pyspark.sql import functions as F

result = (
    df_order_items
    .cube(
        F.when(F.col("unit_price") >= 330, "high").when(F.col("unit_price") >= 190, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(F.sum("quantity").alias("total_quantity"))
    .orderBy("product_id")
)
