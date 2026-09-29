from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["SHIPPED", "PENDING", "RETURNED"]) & (F.col("discount") >= 0.24))
    .groupBy("channel")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .filter(F.col("amount_range") >= 44)
)
