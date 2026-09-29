from pyspark.sql import functions as F

src = (
    df_orders
    .filter((F.col("shipping_fee") >= 11) & ((F.col("status") == "PENDING") | F.col("channel").isin(["web", "store", "mobile"])))
)

summary = (
    src
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    summary
    .filter(F.col("total_amount") < 7200)
    .select("order_month", "total_amount")
)
