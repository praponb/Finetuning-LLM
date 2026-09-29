from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "CREDIT")
    .groupBy("status")
    .pivot("account_type", ["SAVINGS", "CHECKING"])
    .agg(F.count("balance"))
)
