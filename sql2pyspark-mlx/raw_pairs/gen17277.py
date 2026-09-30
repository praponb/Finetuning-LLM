from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("a.account_type") == "CHECKING") & (F.col("a.status") == "CLOSED"))
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("c.segment"),
        F.col("c.age"),
        F.col("c.signup_date"),
    )
    .orderBy("segment")
)
