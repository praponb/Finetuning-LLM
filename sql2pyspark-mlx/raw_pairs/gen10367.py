from pyspark.sql import functions as F

min_opened_date = df_accounts.filter(F.col("status").isin(["CLOSED", "FROZEN", "OPEN"])).agg(F.min("opened_date")).first()[0]

result = (
    df_accounts
    .filter(F.col("opened_date") <= min_opened_date)
    .select("id", "opened_date", "account_type", "balance")
)
