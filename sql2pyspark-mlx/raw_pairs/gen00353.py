from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["LOAN", "CHECKING", "CREDIT"]))
    .select("account_type", "balance", "customer_id")
    .orderBy("account_type")
)
