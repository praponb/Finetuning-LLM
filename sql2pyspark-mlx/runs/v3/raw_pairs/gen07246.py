from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.min("amount").alias("min_amount"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shell"),
    )
)
