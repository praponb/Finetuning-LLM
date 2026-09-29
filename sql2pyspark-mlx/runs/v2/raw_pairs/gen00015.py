from pyspark.sql import functions as F

src = df_transactions.filter(F.col("amount") < 1900)

summary = (
    src
    .groupBy(
        F.when(F.col("amount") >= 3800, "high").when(F.col("amount") >= 1000, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count("amount").alias("amount_count"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    summary
    .filter(F.col("total_amount") >= 17000)
    .select("amount_bucket", "amount_count", "total_amount")
    .orderBy(F.col("amount_count").desc())
)
