from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", F.year("txn_time").alias("txn_year"))
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.max("amount").alias("max_amount"),
    )
)
