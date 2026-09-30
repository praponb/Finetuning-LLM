from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") < "2025-02-26")
    .select("channel")
    .distinct()
)
