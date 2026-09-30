from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.26, 0.31))
    .select("id", "store_id", "order_date")
)
