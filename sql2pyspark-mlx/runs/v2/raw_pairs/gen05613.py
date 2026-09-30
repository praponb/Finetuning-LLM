from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") >= 1900) & (F.col("txn_type") == "FEE") & (F.col("account_id") == 40))
    .groupBy("txn_type", "is_flagged")
    .agg(F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"))
    .orderBy("txn_type")
)
