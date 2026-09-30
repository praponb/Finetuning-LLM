from pyspark.sql import functions as F

avg_quantity = df_order_items.filter(F.col("order_id").isin([25, 31, 33])).agg(F.avg("quantity")).first()[0]

result = (
    df_order_items
    .filter(F.col("quantity") > avg_quantity)
    .select("id", "quantity", "product_id")
    .orderBy("product_id")
)
