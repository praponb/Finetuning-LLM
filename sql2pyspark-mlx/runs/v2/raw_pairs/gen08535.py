from pyspark.sql import functions as F

result = (
    df_accounts
    .agg(F.countDistinct("balance").alias("unique_statuss"))
    .orderBy(F.col("unique_statuss").desc())
)
