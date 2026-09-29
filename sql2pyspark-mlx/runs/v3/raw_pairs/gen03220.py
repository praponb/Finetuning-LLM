from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Amazon")
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
    .filter(F.col("credit_count") < 7)
    .select("txn_month", "credit_count", "merchant_values")
    .orderBy(F.col("txn_month").desc())
)
