from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.max("amount").alias("max_amount"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
    )
    .orderBy(F.col("grab_count").desc_nulls_last())
)
