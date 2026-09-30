from pyspark.sql import functions as F

dim_totals = (
    df_accounts
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .cube(
        F.col("p.segment").alias("segment"),
        F.col("src.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.balance")).alias("min_balance"),
    )
)

result = dim_totals.select("segment", "account_type", "rows_total", "min_balance")
