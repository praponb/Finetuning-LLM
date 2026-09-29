from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([30, 33, 36]))
    .select("status", "channel")
    .distinct()
)
