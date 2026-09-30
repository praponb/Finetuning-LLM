from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id") == 37) | ~F.col("status").isin(["FROZEN", "CLOSED"]))
    .select("id", "account_type")
    .orderBy(F.col("id").desc())
    .limit(5)
)
