from pyspark.sql import functions as F

result = (
    df_accounts
    .select("balance", F.substring("account_type", 1, 2).alias("account_type_prefix"))
    .orderBy(F.col("balance").desc())
)
