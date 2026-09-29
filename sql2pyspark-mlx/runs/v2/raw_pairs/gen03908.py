from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([6, 7, 15]))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)
