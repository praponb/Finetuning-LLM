from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 31)
    .select("status", "channel")
    .distinct()
)
