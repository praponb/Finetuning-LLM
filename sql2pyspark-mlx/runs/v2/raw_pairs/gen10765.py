from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2023-05-29")
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 8)
    .select("txn_quarter", "merchant_values", "cnt")
    .orderBy("txn_quarter", "cnt")
    .limit(3)
)
