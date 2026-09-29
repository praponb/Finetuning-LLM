from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter((F.col("quantity").between(3, 5) & (F.col("quantity") > 6)) | F.col("order_id").isin([1, 27]))
)

order_items_stats = (
    valid_order_items
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    order_items_stats
    .select("product_id", "total_quantity")
    .orderBy(F.col("product_id").desc(), F.col("total_quantity").desc())
)
