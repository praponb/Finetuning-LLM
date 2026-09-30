from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.month(F.col("c.signup_date")).alias("signup_month"),
        F.when(F.col("c.credit_limit") >= 14100, "high").when(F.col("c.credit_limit") >= 6500, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("s.carrier") == "Kerry", 1).otherwise(0)).alias("kerry_count"),
        F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"),
    )
)
