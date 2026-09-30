from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.countDistinct("unit_price").alias("unique_products"),
    )
    .filter(F.col("unique_products") >= 8)
    .select("product_id", "revenue", "unique_products")
)
