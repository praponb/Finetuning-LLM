from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("account_id").isin([2, 8]))
)

agg = (
    filtered_transactions
    .groupBy(
        F.when(F.col("merchant").isin(["Starbucks", "7-Eleven"]), "core").otherwise("other").alias("merchant_group"),
    )
    .agg(
        F.max("amount").alias("max_amount"),
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("amount")).otherwise(0)).alias("7_eleven_amount"),
    )
)

result = (
    agg
    .filter(F.col("7_eleven_amount") > 41)
    .select("merchant_group", "max_amount", "7_eleven_amount")
)
