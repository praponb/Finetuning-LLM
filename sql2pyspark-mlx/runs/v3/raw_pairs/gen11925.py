from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter((F.col("txn_type") != "DEBIT") | (F.to_date("txn_time") < "2023-07-01"))
)

summary = (
    valid_transactions
    .groupBy("txn_type")
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)

result = (
    summary
    .select("txn_type", "merchant_list")
    .orderBy(F.col("txn_type").desc())
)
