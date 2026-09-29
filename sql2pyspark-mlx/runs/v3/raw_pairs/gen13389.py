from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("c.signup_date") > "2025-04-01")
    .groupBy(F.col("c.id"), F.col("c.city"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.round(((100 * F.sum(F.when(F.col("o.channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
    )
    .orderBy("order_count")
)
