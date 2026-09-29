from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.account_type").isin(["CHECKING", "LOAN", "SAVINGS"]))
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("c.name"),
        F.col("c.email"),
    )
    .orderBy("balance")
)
