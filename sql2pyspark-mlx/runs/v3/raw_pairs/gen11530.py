from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isNull() & (F.col("order_date") <= "2024-07-01") & F.col("channel").isin(["mobile", "partner", "store"]))
    .select("discount", "channel")
)
