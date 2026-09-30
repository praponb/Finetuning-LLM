from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") != "Starbucks") & F.col("account_id").isin([17, 24, 26, 28]))
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .agg(
        F.sum(F.when(F.col("merchant") == "Starbucks", 1).otherwise(0)).alias("starbucks_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
    .filter(F.col("starbucks_count") < 56)
)
