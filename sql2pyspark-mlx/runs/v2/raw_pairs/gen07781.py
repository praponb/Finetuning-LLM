from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.year("txn_time").alias("txn_year"), "merchant")
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") < 8)
    .select("txn_year", "merchant", "amount_count")
    .orderBy("txn_year")
)
