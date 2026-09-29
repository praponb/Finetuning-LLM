from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["CHECKING", "CREDIT", "SAVINGS"]))
    .select("status")
    .unionByName(df_payments.filter(F.col("status") == "REFUNDED").select("status"))
)
