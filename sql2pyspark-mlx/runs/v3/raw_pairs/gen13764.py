from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fee"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
    .filter(F.col("pct_fee") >= 30)
    .select("is_flagged", "pct_fee", "merchant_list")
    .orderBy(F.col("pct_fee").desc())
)
