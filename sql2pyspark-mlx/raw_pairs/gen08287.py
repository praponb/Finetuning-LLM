from pyspark.sql import functions as F

rollup_totals = (
    df_accounts
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .groupBy(
        F.col("d.city").alias("city"),
        F.col("e.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.balance")).alias("min_balance"),
    )
)

result = (
    rollup_totals
    .select("city", "account_type", "rows_total", "min_balance")
    .orderBy("min_balance", "rows_total")
)
