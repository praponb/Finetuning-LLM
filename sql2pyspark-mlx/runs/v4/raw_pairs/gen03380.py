from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") < 3800)
    .orderBy("min_amount")
)
