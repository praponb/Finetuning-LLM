from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(8, 16) & (F.col("account_id") == 15) & (F.col("merchant") == "Grab"))
    .groupBy(
        F.when(F.col("merchant").isin(["Grab", "7-Eleven"]), "group_a").otherwise("other").alias("merchant_group"),
    )
    .agg(
        F.count("*").alias("num_transactions"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)
