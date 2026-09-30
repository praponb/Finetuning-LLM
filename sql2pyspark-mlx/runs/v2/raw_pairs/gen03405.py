from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.max("amount").alias("max_amount"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
    .filter(F.col("max_amount") < 2700)
    .orderBy(F.col("max_amount").desc())
)
