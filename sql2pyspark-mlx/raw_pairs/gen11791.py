from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .groupBy(
        F.when(F.col("c.age") >= 71, "high").when(F.col("c.age") >= 30, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.count("*").alias("cnt"))
)
