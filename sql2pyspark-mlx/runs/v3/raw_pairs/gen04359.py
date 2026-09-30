from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") | ~F.col("merchant").isin(["Lazada", "Shell", "7-Eleven"]))
    .groupBy("txn_type")
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_count"),
    )
    .filter(F.col("unique_merchants") > 2)
    .orderBy(F.col("txn_type").desc())
)
