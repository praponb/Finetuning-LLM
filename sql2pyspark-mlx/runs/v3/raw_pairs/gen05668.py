from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(F.countDistinct("amount").alias("unique_accounts"))
)
