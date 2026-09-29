from pyspark.sql import functions as F

src = df_order_items.filter(F.col("unit_price").between(160, 270))

product_id_summary = (
    src
    .groupBy("product_id")
    .agg(F.count("unit_price").alias("unit_price_count"))
)

result = product_id_summary.select("product_id", "unit_price_count")
