from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") != "RETURNED") & F.col("channel").isin(["store", "mobile", "partner"]))
    .groupBy("channel")
    .agg(F.min("order_date").alias("first_order_date"))
)
