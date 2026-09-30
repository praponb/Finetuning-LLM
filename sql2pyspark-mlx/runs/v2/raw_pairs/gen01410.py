from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(F.sum(F.col("p.amount")).alias("total_amount"))
    .orderBy(F.col("total_amount").desc(), "signup_year")
    .limit(10)
)
