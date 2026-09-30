from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") > 3900) & (F.to_date("txn_time") >= "2025-03-01"))
    .groupBy("is_flagged")
    .agg(
        F.count("amount").alias("amount_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)
