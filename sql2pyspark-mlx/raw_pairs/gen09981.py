from pyspark.sql import functions as F

recent_transactions = df_transactions.filter(F.col("account_id").isin([21, 24, 25]))

agg = (
    recent_transactions
    .groupBy("txn_type")
    .agg(
        F.max("amount").alias("max_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)

result = (
    agg
    .filter(F.col("max_amount") >= 2600)
    .orderBy(F.col("txn_type_values").desc())
)
