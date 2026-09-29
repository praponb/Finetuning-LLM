from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") != "FEE") & (F.col("txn_time") < "2023-08-30") & (F.col("merchant") != "Amazon"))
    .groupBy("merchant")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
    .orderBy(F.col("merchant_values").desc())
)
