from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id").isin([28, 30, 36, 38]) & (F.col("unit_price") > 270)) | F.col("product_id").isin([28, 36]))
    .groupBy("order_id")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("unit_price").alias("unique_orders"),
    )
    .filter(F.col("row_count") < 5)
    .orderBy("unique_orders")
)
