from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(
        F.countDistinct("amount").alias("unique_accounts"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") < 66)
    .select("merchant", "unique_accounts", "avg_amount")
    .orderBy(F.col("unique_accounts").desc())
)
