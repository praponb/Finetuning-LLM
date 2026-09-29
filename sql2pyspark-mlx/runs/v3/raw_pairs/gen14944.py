from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 53000)
    .groupBy("status")
    .pivot("account_type", ["CREDIT", "SAVINGS"])
    .agg(F.sum("balance"))
    .orderBy("status")
)
