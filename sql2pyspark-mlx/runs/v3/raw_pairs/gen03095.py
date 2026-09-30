from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(~F.col("a.status").isin(["CLOSED", "OPEN"]))
    .groupBy(F.col("a.status"))
    .agg(
        F.sum(F.col("t.amount")).alias("total_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("t.merchant")))).alias("merchant_values"),
    )
    .orderBy("merchant_values")
)
