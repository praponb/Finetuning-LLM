from pyspark.sql import functions as F

valid_orders = df_orders.filter((F.col("amount") >= 1570) & (F.col("channel") == "partner"))

agg = (
    valid_orders
    .groupBy("status")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = agg.orderBy(F.col("status").desc(), "amount_range")
