from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.merchant") != "7-Eleven")
    .groupBy(F.year(F.col("a.opened_date")).alias("opened_year"))
    .agg(
        F.count("*").alias("num_transactions"),
        F.sort_array(F.collect_set(F.col("t.txn_type"))).alias("txn_type_list"),
    )
    .orderBy("opened_year")
)
