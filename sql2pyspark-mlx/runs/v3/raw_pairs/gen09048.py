from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 1410)
    .select("id", "order_date")
)
