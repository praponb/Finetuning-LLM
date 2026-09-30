from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(36000, 66000))
    .select("id", "opened_date", "account_type")
    .unionByName(
        df_accounts
        .filter(F.col("status").isin(["CLOSED", "FROZEN"]))
        .select("id", "opened_date", "account_type")
    )
    .orderBy("id")
)
