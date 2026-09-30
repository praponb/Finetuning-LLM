from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.status").isin(["FROZEN", "CLOSED", "OPEN"]) | (F.col("a.opened_date") <= "2024-10-01"))
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("customer_id").desc())
)
