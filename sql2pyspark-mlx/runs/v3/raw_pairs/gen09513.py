from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter(~F.col("a.status").isin(["OPEN", "FROZEN", "CLOSED"]) | (F.year(F.col("a.opened_date")) >= 2025))
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("c.name"))
    .orderBy("customer_id")
)
