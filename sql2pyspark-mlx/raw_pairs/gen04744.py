from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "CREDIT") & ((F.col("balance") > 22000) | (F.col("account_type") != "CHECKING")))
    .withColumn("balance_whole", F.ceil("balance"))
    .withColumn("account_type_length", F.length("account_type"))
    .orderBy(F.col("balance").desc())
)
