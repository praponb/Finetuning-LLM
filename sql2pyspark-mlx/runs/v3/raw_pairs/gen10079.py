from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2023-07-13", "2025-06-01"))
    .select("channel")
)
