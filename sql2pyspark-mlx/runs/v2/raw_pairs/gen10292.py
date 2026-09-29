from pyspark.sql import functions as F

min_order_date = df_orders.agg(F.min("order_date")).first()[0]

result = (
    df_orders
    .filter(F.col("order_date") == min_order_date)
    .select("id", "order_date")
    .orderBy(F.col("id").desc(), F.col("order_date").desc())
)
