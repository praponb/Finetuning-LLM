from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("a.opened_date") <= "2023-07-01") & F.col("c.city").isin(["Chiang Mai", "London", "Berlin"]))
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("c.is_active"),
        F.col("c.age"),
        F.col("c.city"),
    )
)
