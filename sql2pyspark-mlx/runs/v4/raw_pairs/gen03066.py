from pyspark.sql import functions as F

src = (
    df_order_items
    .filter(F.col("product_id").isNotNull() | (F.col("unit_price") >= 340))
)

agg = (
    src
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.countDistinct("unit_price").alias("unique_products"))
)

result = (
    agg
    .filter(F.col("unique_products") >= 5)
    .orderBy("quantity_bucket")
    .limit(5)
)
