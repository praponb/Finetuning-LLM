from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"),
        F.col("c.is_active"),
    )
    .agg(
        F.avg(F.col("o.amount")).alias("avg_amount"),
        F.count(F.when(F.col("o.status") == "RETURNED", 1)).alias("returned_count"),
    )
    .filter(F.col("avg_amount") >= 1490)
)
