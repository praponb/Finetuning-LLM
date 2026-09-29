from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("amount") < 3300)
    .withColumn("txn_type_group", F.when(F.col("txn_type").isin(["DEBIT", "REFUND"]), "Group A").otherwise("Other"))
)

agg = (
    valid_transactions
    .groupBy("txn_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
    )
)

result = (
    agg
    .select("txn_type", "merchant_values", "amazon_count")
    .orderBy(F.col("amazon_count").desc())
)
