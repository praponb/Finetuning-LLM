from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("o.shipping_fee") > 35)
    .groupBy(
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"),
        F.col("c.is_active"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("o.status"))).alias("status_list"),
        F.countDistinct(F.col("o.discount")).alias("unique_channels"),
    )
    .orderBy("unique_channels")
)
