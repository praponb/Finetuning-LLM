from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "inner")
    .filter(F.col("accounts.status") == "OPEN")
    .groupBy(
        F.col("accounts.account_type"),
        F.quarter(F.col("accounts.opened_date")).alias("opened_quarter"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("transactions.txn_type")))).alias("txn_type_values"),
        F.count("*").alias("total_count"),
    )
    .orderBy(F.col("total_count").desc())
)
