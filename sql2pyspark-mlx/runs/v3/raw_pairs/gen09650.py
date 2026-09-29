from pyspark.sql import functions as F

src = (
    df_transactions
    .filter((F.col("txn_type") == "FEE") & ((F.col("txn_time") >= "2024-03-01") | (F.to_date("txn_time") >= "2025-01-01")))
)

amount_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("amount") >= 4500, "high").when(F.col("amount") >= 600, "mid").otherwise("low").alias("amount_bucket"),
        F.when(F.col("txn_type").isin(["CREDIT", "DEBIT"]), "core").otherwise("other").alias("txn_type_group"),
    )
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    amount_bucket_summary
    .filter(F.col("amount_count") > 4)
    .select("amount_bucket", "txn_type_group", "merchant_list", "amount_count")
    .orderBy(F.col("txn_type_group").desc(), "amount_bucket")
    .limit(5)
)
