from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("product_id") == 13) & F.col("order_id").isin([1, 4, 17, 21])) | F.col("product_id").isNull())
    .groupBy("order_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.max("unit_price").alias("max_unit_price"),
    )
)
