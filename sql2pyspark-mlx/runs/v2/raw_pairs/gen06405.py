from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "SAVINGS", "LOAN"]))
    .select("balance", "customer_id", "status")
    .orderBy("status")
)
