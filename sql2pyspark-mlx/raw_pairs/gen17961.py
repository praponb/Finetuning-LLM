from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "LOAN", "SAVINGS"]))
    .select("opened_date")
    .subtract(df_accounts.filter(F.col("status") == "CLOSED").select("opened_date"))
)
