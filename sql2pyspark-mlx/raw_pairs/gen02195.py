from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "OPEN"]))
    .select("balance", "status", "customer_id")
)
