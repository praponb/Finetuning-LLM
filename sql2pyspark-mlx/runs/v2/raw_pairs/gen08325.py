from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "OPEN"]))
    .groupBy("customer_id")
    .agg(F.count("balance").alias("balance_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("C"), F.col("c.id") == F.col("C.customer_id"), "left")
    .select(F.col("c.name"), F.col("c.country"), F.col("C.balance_count"))
    .orderBy(F.col("name").desc(), F.col("balance_count").desc())
)
