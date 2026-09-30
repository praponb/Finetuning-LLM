from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("a.status").isin(["OPEN", "FROZEN"]) | (F.col("c.signup_date") >= "2023-12-18"))
    .select(
        F.col("a.opened_date"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("c.segment"),
        F.col("c.name"),
    )
)
