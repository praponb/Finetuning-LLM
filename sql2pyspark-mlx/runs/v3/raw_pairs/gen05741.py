from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") <= 0.11) | F.col("order_date").between("2023-05-30", "2023-11-19"))
    .select("id", "shipping_fee", "channel")
)
