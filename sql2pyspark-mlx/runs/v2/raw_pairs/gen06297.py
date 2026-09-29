from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.age") < 56)
    .groupBy(
        F.when(F.col("customers.age") >= 60, "high").when(F.col("customers.age") >= 29, "mid").otherwise("low").alias("age_bucket"),
        F.month(F.col("customers.signup_date")).alias("signup_month"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("orders.status"))).alias("status_list"),
        F.round(F.avg(F.col("orders.shipping_fee")), 2).alias("avg_shipping_fee"),
    )
)
