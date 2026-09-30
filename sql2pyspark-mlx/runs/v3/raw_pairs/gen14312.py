from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.customer_id").isNull() & F.col("a.opened_date").between("2023-03-01", "2024-03-01"))
    .select(F.col("a.id"), F.col("a.opened_date"), F.col("c.segment"))
)
