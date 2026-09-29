from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "CHECKING"]))
    .groupBy("customer_id")
    .agg(F.min("balance").alias("min_balance"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.min_balance") >= 57000)
    .select(F.col("c.id"), F.col("c.signup_date"), F.col("cs.min_balance"))
)
