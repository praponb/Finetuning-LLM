from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["CREDIT", "REFUND", "FEE"]))
    .groupBy("account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.min("amount").alias("min_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
    .orderBy("min_amount")
)
