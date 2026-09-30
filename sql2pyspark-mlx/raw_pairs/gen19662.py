from pyspark.sql import functions as F

avg_quantity = df_order_items.filter(F.col("product_id").isin([31, 32, 34, 39])).agg(F.avg("quantity")).first()[0]

result = (
    df_order_items
    .filter(F.col("quantity") > avg_quantity)
    .select("id", "quantity", "product_id", "unit_price")
)
