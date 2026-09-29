from pyspark.sql import functions as F

src = (
    df_order_items
    .filter(F.col("product_id").isin([3, 15, 29]))
    .withColumn("unit_price_whole", F.ceil("unit_price"))
)

summary = (
    src
    .groupBy("product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.sum("unit_price_whole").alias("total_unit_price_whole"),
    )
)

result = (
    summary
    .select("product_id", "unique_products")
    .orderBy(F.col("product_id").desc(), "unique_products")
)
