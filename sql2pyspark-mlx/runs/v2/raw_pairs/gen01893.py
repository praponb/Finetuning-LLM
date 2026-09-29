from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.count("amount").alias("amount_count"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
    .orderBy(F.col("amount_count").desc(), F.col("txn_quarter").desc())
)
