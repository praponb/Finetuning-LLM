from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["CREDIT", "FEE"]) & (F.col("account_id") == 37))
    .rollup(F.date_trunc("MONTH", "txn_time").alias("txn_month"), "merchant")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.countDistinct("amount").alias("unique_txn_types"),
    )
    .orderBy(F.col("total_amount").desc())
)
