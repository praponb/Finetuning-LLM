from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg((F.max("discount") - F.min("discount")).alias("discount_range"))
)
