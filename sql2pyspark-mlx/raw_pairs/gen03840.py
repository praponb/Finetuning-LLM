from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("account_type").isin(["SAVINGS", "LOAN"]), "Group A").otherwise("Other").alias("account_type_group"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_clean"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < F.col("s.opened_date")), "left_anti")
)
