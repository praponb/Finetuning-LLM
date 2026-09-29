from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["CREDIT", "LOAN", "CHECKING"]) & F.col("account_type").isin(["SAVINGS", "CHECKING"]))
    .withColumn("account_type_length", F.length("account_type"))
)
