from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .pivot("account_type", ["CHECKING", "SAVINGS", "LOAN", "CREDIT"])
    .agg(F.sum("balance"))
)
