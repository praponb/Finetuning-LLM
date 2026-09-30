from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3900, "high").when(F.col("amount") >= 2000, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count("*").alias("num_transactions"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("num_transactions") > 8)
    .select("amount_bucket", "num_transactions", "total_amount")
)
