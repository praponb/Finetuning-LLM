from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") | (F.col("txn_type") == "REFUND"))
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.sum("amount").alias("total_amount"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
    )
    .orderBy(F.col("fee_count").desc())
)
