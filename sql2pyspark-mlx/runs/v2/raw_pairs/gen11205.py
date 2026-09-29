from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .groupBy("customer_id")
    .agg(F.min("discount").alias("min_discount"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.signup_date"), F.col("C.min_discount"))
)
