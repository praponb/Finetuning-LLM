from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([1, 3, 12]) & (F.col("discount") < 0.36))
    .select("id", "channel")
)
