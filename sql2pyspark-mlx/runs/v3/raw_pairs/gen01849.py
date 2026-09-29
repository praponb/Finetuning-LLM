from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("is_flagged", "account_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Starbucks", 1).otherwise(0))) / F.count("*")), 1).alias("pct_starbucks"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") > 3100)
    .drop("max_amount")
)
