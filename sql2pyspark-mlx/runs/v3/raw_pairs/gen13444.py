from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "outer")
    .groupBy(
        F.date_format(F.col("customers.signup_date"), "yyyy-MM").alias("signup_month"),
    )
    .agg(F.avg(F.col("orders.discount")).alias("avg_discount"))
    .filter(F.col("avg_discount") > 0.19)
    .orderBy("signup_month")
)
