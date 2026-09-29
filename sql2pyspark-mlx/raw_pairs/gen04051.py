from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "OPEN"]) & F.col("status").isin(["FROZEN", "CLOSED", "OPEN"]))
    .select("id", "opened_date", "account_type")
    .orderBy(F.col("opened_date").desc(), "id")
    .limit(10)
)
