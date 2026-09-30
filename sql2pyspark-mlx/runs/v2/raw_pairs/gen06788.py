from pyspark.sql import functions as F

base = (
    df_transactions
    .filter((F.col("account_id") == 3) & F.col("account_id").isin([1, 8, 12]) & F.col("txn_type").isin(["DEBIT", "CREDIT"]))
)

txn_type_summary = (
    base
    .groupBy("txn_type", F.to_date("txn_time").alias("txn_day"))
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    txn_type_summary
    .filter(F.col("min_amount") > 3600)
    .orderBy("txn_type")
)
