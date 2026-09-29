from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["country", "account_type"], ["country"]],
        F.col("d.country").alias("country"),
        F.col("a.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.balance")).alias("max_balance"),
    )
    .orderBy(F.col("max_balance").desc_nulls_first())
)
