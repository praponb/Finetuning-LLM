from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("orders.order_date") > "2024-04-11")
    .groupBy(
        F.when(F.col("customers.age") >= 69, "high").when(F.col("customers.age") >= 33, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.round(F.avg(F.col("orders.amount")), 2).alias("avg_amount"))
)
