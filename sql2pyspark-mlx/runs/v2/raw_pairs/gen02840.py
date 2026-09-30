from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Starbucks") & (F.col("merchant") != "7-Eleven"))
    .groupBy(
        F.hour("txn_time").alias("txn_hour"),
        F.when(F.col("amount") >= 3900, "high").when(F.col("amount") >= 1200, "mid").otherwise("low").alias("amount_bucket"),
        "merchant",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
        F.count("amount").alias("amount_count"),
        F.min("txn_time").alias("first_txn_time"),
    )
    .filter(F.col("amount_count") > 1)
    .orderBy(F.col("amount_count").desc(), "txn_hour", "amount_bucket", "merchant")
    .limit(1)
)
