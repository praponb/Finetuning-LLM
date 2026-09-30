from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .filter(F.col("balance") >= 27000)
    .groupBy("customer_id")
    .agg(F.min("opened_date").alias("first_opened_date"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.age"), F.col("t.first_opened_date"))
)
