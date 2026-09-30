from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter((F.col("unit_price") < 310) & F.col("unit_price").isNotNull())

product_id_summary = (
    filtered_order_items
    .groupBy("product_id")
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
)

result = product_id_summary.select("product_id", "stddev_unit_price")
