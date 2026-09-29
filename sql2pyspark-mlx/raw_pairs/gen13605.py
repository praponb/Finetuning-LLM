from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
    .filter(F.col("amazon_count") > 1)
    .orderBy(F.col("num_txn_types").desc())
)
