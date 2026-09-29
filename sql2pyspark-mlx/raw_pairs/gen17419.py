from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter((F.col("unit_price") >= 340) & F.col("product_id").isNull())

order_id_summary = (
    filtered_order_items
    .groupBy("order_id")
    .agg(F.max("unit_price").alias("max_unit_price"))
)

result = order_id_summary.select("order_id", "max_unit_price")
