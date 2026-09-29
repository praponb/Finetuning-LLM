from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~((F.col("quantity") == 4) & F.col("order_id").isin([15, 24])))
    .groupBy("product_id")
    .agg(F.min("unit_price").alias("min_unit_price"))
    .orderBy("min_unit_price", F.col("product_id").desc())
    .limit(1)
)
