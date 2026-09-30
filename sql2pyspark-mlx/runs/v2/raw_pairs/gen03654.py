from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1500, 3900))
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)
