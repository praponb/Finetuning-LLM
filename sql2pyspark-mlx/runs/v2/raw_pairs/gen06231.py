from pyspark.sql import functions as F

valid_accounts = df_accounts.filter(F.col("status").isin(["OPEN", "CLOSED"]))

customer_id_summary = (
    valid_accounts
    .groupBy("customer_id")
    .agg(F.max("balance").alias("max_balance"))
)

result = customer_id_summary.filter(F.col("max_balance") >= 27000)
