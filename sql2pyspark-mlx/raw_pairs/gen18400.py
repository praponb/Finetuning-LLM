from pyspark.sql import functions as F

filtered_transactions = df_transactions.filter(F.col("amount").between(1700, 3800))

amount_bucket_summary = (
    filtered_transactions
    .groupBy(
        F.when(F.col("amount") >= 4100, "high").when(F.col("amount") >= 1600, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.min("amount").alias("min_amount"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)

result = (
    amount_bucket_summary
    .filter(F.col("min_amount") >= 3100)
    .orderBy("min_amount")
)
