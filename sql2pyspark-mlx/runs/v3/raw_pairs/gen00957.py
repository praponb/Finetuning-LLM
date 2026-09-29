from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(F.col("a.account_type"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("t.merchant")))).alias("merchant_values"),
    )
)
