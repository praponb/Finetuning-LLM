from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") <= 0.11) & (F.col("order_date") <= "2023-08-14"))
)
