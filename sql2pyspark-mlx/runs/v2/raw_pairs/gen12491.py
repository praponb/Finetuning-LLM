from pyspark.sql import functions as F

base = (
    df_transactions
    .filter((F.to_date("txn_time") >= "2024-08-04") & F.col("merchant").isin(["Lazada", "Amazon"]))
)

summary = (
    base
    .groupBy("merchant")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"),
    )
)

result = summary.select("merchant", "txn_type_values", "debit_count")
