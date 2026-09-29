from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.countDistinct("unit_price").alias("unique_products"),
    )
    .filter(F.col("unique_products") >= 4)
    .select("order_id", "max_unit_price", "unique_products")
)
