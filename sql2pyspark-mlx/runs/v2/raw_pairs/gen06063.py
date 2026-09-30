from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.email"), F.col("t.row_count"))
    .orderBy("email", "row_count")
)
