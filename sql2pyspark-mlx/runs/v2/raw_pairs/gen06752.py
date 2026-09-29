from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .groupBy("customer_id")
    .agg(F.round(F.avg("discount"), 2).alias("avg_discount"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.avg_discount") >= 59)
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.country"),
        F.col("t.avg_discount"),
    )
)
