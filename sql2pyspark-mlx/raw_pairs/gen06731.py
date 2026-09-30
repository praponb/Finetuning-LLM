from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged", "txn_type")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
    .filter(F.col("amount_range") >= 44)
    .select("is_flagged", "txn_type", "amount_range", "merchant_list")
    .orderBy("amount_range")
)
