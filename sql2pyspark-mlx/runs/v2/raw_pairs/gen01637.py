from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "OPEN") | ~F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]))
    .select("customer_id", F.abs("balance").alias("abs_balance"))
    .orderBy(F.col("customer_id").desc(), F.col("abs_balance").desc())
)
