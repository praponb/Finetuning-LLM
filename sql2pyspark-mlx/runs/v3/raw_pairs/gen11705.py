from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged", "merchant")
    .agg(
        F.max("amount").alias("max_amount"),
        F.countDistinct("amount").alias("unique_accounts"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .filter(F.col("unique_accounts") < 2)
)
