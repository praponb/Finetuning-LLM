from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.year(F.col("a.opened_date")).alias("opened_year"),
        F.col("a.account_type"),
    )
    .agg(
        F.sum(F.when(F.col("t.txn_type") == "FEE", F.col("t.amount")).otherwise(0)).alias("fee_amount"),
    )
    .filter(F.col("fee_amount") > 30)
    .orderBy(F.col("fee_amount").desc())
)
