from pyspark.sql import functions as F

max_order_date = df_orders.filter(F.col("order_date").between("2024-08-01", "2025-03-01")).agg(F.max("order_date")).first()[0]

result = (
    df_orders
    .filter(F.col("order_date") == max_order_date)
    .select("id", "order_date", "channel", "amount")
)
