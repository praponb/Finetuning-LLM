from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & F.col("account_id").isin([7, 27, 33]))
    .groupBy("txn_type", "merchant")
    .agg(
        F.count("*").alias("total_count"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Amazon", 1).otherwise(0))) / F.count("*")), 1).alias("pct_amazon"),
    )
    .orderBy(F.col("merchant").desc(), "txn_type")
    .limit(3)
)
