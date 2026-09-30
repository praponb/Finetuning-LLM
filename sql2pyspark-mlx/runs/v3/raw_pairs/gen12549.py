from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.avg("balance").alias("avg_balance"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("C"), F.col("c.id") == F.col("C.customer_id"), "left")
    .select(F.col("c.country"), F.col("c.name"), F.col("C.avg_balance"))
    .orderBy(F.col("avg_balance").desc())
)
