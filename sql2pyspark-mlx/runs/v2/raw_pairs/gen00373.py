from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (F.col("channel") != "web") & F.col("discount").between(0.18, 0.27))
)
