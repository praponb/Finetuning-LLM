from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Grab")
    .groupBy("txn_type")
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.min("amount").alias("min_amount"),
        F.countDistinct("amount").alias("unique_accounts"),
    )
    .filter(F.col("unique_accounts") > 1)
    .drop("unique_accounts")
)
