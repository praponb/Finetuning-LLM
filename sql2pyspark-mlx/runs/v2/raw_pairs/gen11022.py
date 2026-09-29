from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(~F.col("is_flagged") & (F.col("txn_type").isin(["REFUND", "FEE"]) | F.hour("txn_time").between(9, 15)))
)

transactions_stats = (
    base
    .groupBy("merchant")
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_count"),
    )
)

result = (
    transactions_stats
    .filter(F.col("starbucks_count") > 5)
    .select("merchant", "txn_type_list", "starbucks_count")
    .orderBy(F.col("starbucks_count").desc(), "merchant")
    .limit(10)
)
