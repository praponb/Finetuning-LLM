from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("quantity") < 6) & (F.col("order_id") == 5)) | (F.col("unit_price") >= 270))
    .groupBy("product_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
)
