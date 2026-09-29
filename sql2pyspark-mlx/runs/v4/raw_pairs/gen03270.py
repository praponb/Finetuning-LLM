from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "CANCELLED") & F.col("channel").isNotNull())
    .groupBy("channel")
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.min("amount").alias("min_amount"),
    )
    .orderBy("last_order_date", "min_amount")
)
