from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([9, 20, 32]) & F.col("account_id").isin([13, 19, 28]) & F.col("account_id").isin([7, 13, 14, 20]))
    .groupBy("is_flagged")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)
