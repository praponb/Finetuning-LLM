from pyspark.sql import functions as F

filtered_transactions = df_transactions.filter(F.col("account_id").isin([7, 13]))

summary = (
    filtered_transactions
    .groupBy(
        F.when(F.col("merchant").isin(["Lazada", "Shell"]), "priority").otherwise("other").alias("merchant_group"),
        F.when(F.col("amount") >= 3400, "high").when(F.col("amount") >= 1000, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    summary
    .filter(F.col("max_amount") > 2200)
    .orderBy(F.col("max_amount").desc())
)
