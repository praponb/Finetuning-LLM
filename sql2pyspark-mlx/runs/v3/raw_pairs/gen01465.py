from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(((F.col("merchant") == "Shell") & (F.col("amount") < 3100)) | F.col("is_flagged"))
    .groupBy(
        F.when(F.col("merchant").isin(["7-Eleven", "Starbucks"]), "core").otherwise("other").alias("merchant_group"),
    )
    .agg(
        F.sum(F.when(F.col("merchant") == "Grab", F.col("amount")).otherwise(0)).alias("grab_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
    .filter(F.col("stddev_amount") >= 27)
    .orderBy("merchant_group")
)
