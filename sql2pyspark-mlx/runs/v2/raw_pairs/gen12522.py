from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") < "2023-09-01") & F.col("amount").between(1900, 2200))
    .groupBy("account_id")
    .agg(
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
    .filter(F.col("lazada_count") >= 5)
    .orderBy(F.col("account_id").desc(), "txn_type_values")
)
