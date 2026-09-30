from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("account_type_group", F.when(F.col("account_type").isin(["CHECKING", "SAVINGS"]), "Priority").otherwise("Other"))
)
