from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .groupBy(
        F.when(F.col("orders.discount") >= 0.39, "high").when(F.col("orders.discount") >= 0.17, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(F.count(F.col("payments.amount")).alias("amount_count"))
    .orderBy(F.col("discount_bucket").desc(), F.col("amount_count").desc())
)
