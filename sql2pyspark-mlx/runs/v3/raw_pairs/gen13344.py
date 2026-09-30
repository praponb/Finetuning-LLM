from pyspark.sql import functions as F

src = df_order_items.filter(F.col("product_id") == 30)

agg = (
    src
    .groupBy("order_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
)

result = agg
