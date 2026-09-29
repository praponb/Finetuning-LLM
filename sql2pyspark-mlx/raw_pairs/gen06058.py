from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("status_group", F.when(F.col("status").isin(["CLOSED", "OPEN"]), "Core").otherwise("Other"))
)
