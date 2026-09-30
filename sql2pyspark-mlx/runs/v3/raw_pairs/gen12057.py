from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("a.opened_date").between("2024-02-01", "2024-12-01"))
    .groupBy(F.col("c.segment"))
    .agg(
        F.count("*").alias("row_count"),
        F.min(F.col("a.balance")).alias("min_balance"),
    )
    .filter(F.col("min_balance") > 58000)
    .orderBy("row_count")
)
