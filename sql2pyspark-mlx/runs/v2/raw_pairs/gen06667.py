from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") < "2024-03-06") & F.col("store_id").isin([6, 24, 35]))
    .orderBy("status")
)
