from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.signup_date") < "2023-03-01")
    .groupBy(
        F.when(F.col("c.age") >= 70, "high").when(F.col("c.age") >= 33, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.sort_array(F.collect_set(F.col("s.status"))).alias("status_list"))
)
