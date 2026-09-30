from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "FROZEN"]) | F.col("balance").between(34000, 78000))
    .select("id", "opened_date", "account_type")
    .orderBy("id")
)
