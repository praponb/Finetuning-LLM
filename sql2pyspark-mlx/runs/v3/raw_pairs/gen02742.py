from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .groupBy(
        F.year(F.col("a.opened_date")).alias("opened_year"),
        F.col("a.account_type"),
    )
    .agg(
        F.countDistinct(F.col("t.amount")).alias("unique_txn_types"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("unique_txn_types").desc())
)
