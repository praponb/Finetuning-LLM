from pyspark.sql import functions as F

recent_transactions = df_transactions.filter((F.col("txn_type") == "DEBIT") & F.col("is_flagged"))

summary = (
    recent_transactions
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)

result = (
    summary
    .select("account_id", "merchant_values")
    .orderBy("merchant_values", "account_id")
)
