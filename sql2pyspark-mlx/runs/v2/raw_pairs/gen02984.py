from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(6, 9))
    .cube("merchant", "is_flagged")
    .agg(
        F.max("amount").alias("max_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
    .orderBy("max_amount")
)
