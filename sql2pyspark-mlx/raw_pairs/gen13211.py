from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([19, 20, 35]) | (F.col("merchant") == "Shell"))
    .groupBy("is_flagged")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
    .orderBy("txn_type_values")
)
