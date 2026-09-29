from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "outer")
    .groupBy(
        F.when(F.col("c.age") >= 69, "high").when(F.col("c.age") >= 34, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.sort_array(F.collect_set(F.col("o.channel"))).alias("channel_list"))
    .orderBy(F.col("age_bucket").desc())
)
