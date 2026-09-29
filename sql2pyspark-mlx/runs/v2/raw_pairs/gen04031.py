from pyspark.sql import functions as F

base = df_transactions.filter(F.col("txn_type") == "REFUND")

summary = (
    base
    .groupBy("txn_type")
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    summary
    .filter(F.col("avg_amount") >= 2400)
    .select("txn_type", "txn_type_list", "avg_amount")
)
