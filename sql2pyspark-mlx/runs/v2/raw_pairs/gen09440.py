from pyspark.sql import functions as F

min_quantity = df_order_items.filter(F.col("order_id").isin([4, 13, 23, 35])).agg(F.min("quantity")).first()[0]

result = (
    df_order_items
    .filter(F.col("quantity") < min_quantity)
    .select("id", "quantity", "product_id", "unit_price")
)
