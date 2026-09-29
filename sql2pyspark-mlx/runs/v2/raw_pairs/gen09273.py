from pyspark.sql import functions as F

avg_quantity = df_order_items.filter(F.col("unit_price") >= 250).agg(F.avg("quantity")).first()[0]

result = (
    df_order_items
    .filter(F.col("quantity") >= avg_quantity)
    .select("id", "quantity")
    .orderBy(F.col("quantity").desc(), "id")
)
