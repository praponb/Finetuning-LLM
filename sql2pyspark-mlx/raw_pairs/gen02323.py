from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~(~F.col("account_type").isin(["CHECKING", "CREDIT", "LOAN"]) & F.col("account_type").isin(["CHECKING", "CREDIT"]) & F.col("customer_id").isin([2, 30])))
    .select("id", "status", "opened_date", "account_type")
)
