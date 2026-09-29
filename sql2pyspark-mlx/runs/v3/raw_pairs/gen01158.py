from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & F.col("account_type").isin(["CREDIT", "CHECKING", "LOAN"]) & F.col("status").isin(["OPEN", "FROZEN", "CLOSED"]))
    .select("id", "account_type", "status")
)
