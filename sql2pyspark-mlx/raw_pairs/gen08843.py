from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([5, 6, 12]))
    .select("order_date", "amount")
)
