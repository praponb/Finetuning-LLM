from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("amount") <= 2300)
    .withColumn("txn_type_merchant", F.concat("txn_type", F.lit("/"), "merchant"))
)

transactions_stats = (
    valid_transactions
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"))
)

result = transactions_stats.orderBy("txn_month")
