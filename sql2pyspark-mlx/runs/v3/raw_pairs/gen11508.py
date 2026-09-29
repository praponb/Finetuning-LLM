from pyspark.sql import functions as F

max_quantity = df_order_items.filter(F.col("unit_price").isNotNull()).agg(F.max("quantity")).first()[0]

result = (
    df_order_items
    .filter(F.col("quantity") >= max_quantity)
    .select("id", "quantity", "unit_price", "order_id")
)
