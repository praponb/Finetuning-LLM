from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(
        F.max("amount").alias("max_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 4)
    .drop("cnt")
    .orderBy("txn_type_values", "is_flagged")
    .limit(20)
)
