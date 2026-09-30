from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity").between(4, 6) & F.col("order_id").isin([1, 8, 30, 40])) | F.col("order_id").isin([23, 31]))
    .groupBy("product_id", "order_id")
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
)
