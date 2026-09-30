from pyspark.sql import functions as F

result = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.unit_price").between(200, 270)), "left_anti")
)
