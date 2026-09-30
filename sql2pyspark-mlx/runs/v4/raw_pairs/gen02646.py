from pyspark.sql import functions as F

avg_quantity = df_order_items.filter(F.col("unit_price") <= 130).agg(F.avg("quantity")).first()[0]

result = (
    df_order_items
    .filter(F.col("quantity") >= avg_quantity)
    .select("id", "quantity", "order_id")
    .orderBy(F.col("order_id").desc(), "id")
    .limit(5)
)
