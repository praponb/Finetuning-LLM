from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.email").like("%@GMAIL.COM") & (F.col("c.country") == "UK"))
    .select(
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.col("c.age"),
        F.col("c.city"),
        F.col("c.is_active"),
    )
    .orderBy("opened_date")
)
