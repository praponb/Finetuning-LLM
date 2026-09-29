from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") <= 3000) & (F.col("merchant") == "Starbucks") & F.col("txn_type").isin(["REFUND", "DEBIT", "FEE"]))
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") > 3400)
    .drop("min_amount")
)
