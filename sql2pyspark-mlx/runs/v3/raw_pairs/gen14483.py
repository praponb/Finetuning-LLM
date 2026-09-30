from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") | (F.col("merchant") == "Shell"))
    .groupBy(
        F.when(F.col("amount") >= 3600, "high").when(F.col("amount") >= 900, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") > 4)
)
