from pyspark.sql import functions as F

src = df_orders.filter(F.col("customer_id").isNull())

summary = (
    src
    .groupBy("channel")
    .agg(F.min("order_date").alias("min_order_date"))
)

result = (
    summary
    .orderBy("channel", "min_order_date")
    .limit(5)
)
