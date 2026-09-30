from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(2100, 3400))
    .groupBy("merchant", F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.count("amount").alias("amount_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") >= 7000)
    .drop("total_amount")
)
