from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter(F.col("txn_type").isin(["DEBIT", "REFUND"]) & (F.col("txn_time") < "2023-12-01"))
)

merchant_summary = (
    recent_transactions
    .groupBy("merchant")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)

result = (
    merchant_summary
    .filter(F.col("total_amount") > 13000)
    .orderBy(F.col("total_amount").desc(), "merchant")
    .limit(20)
)
