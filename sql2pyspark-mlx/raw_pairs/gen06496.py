from pyspark.sql import functions as F

avg_unit_price = df_order_items.filter(F.col("order_id") == 12).agg(F.avg("unit_price")).first()[0]

result = (
    df_order_items
    .filter(F.col("unit_price") > avg_unit_price)
    .select("id", "unit_price", "quantity", "product_id")
)
