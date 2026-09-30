from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & F.col("merchant").isin(["Shell", "Lazada"]) & F.col("amount").between(1700, 3100))
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("amount")).otherwise(0)).alias("amazon_amount"),
    )
)
