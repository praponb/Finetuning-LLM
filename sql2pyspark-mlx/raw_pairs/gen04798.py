from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.year("opened_date") == 2023)
    .groupBy("account_type")
    .pivot("status", ["CLOSED", "OPEN", "FROZEN"])
    .agg(F.sum("balance"))
)
