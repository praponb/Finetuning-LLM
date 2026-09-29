from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("a.status").isin(["OPEN", "FROZEN"]) & (F.col("a.balance") > 65000))
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("c.name"),
        F.col("c.email"),
        F.col("c.age"),
    )
    .orderBy("name")
)
