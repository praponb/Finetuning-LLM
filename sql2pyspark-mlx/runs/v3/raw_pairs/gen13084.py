from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
    .filter(F.col("credit_count") > 61)
    .select("merchant", "credit_count", "txn_type_list")
)
