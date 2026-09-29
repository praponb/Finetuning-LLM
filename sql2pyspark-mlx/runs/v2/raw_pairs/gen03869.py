from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.city") != "Bangkok")
    .select(
        F.col("c.email"),
        F.col("a.balance"),
        F.add_months(F.col("a.opened_date"), 1).alias("opened_date_plus_months"),
    )
    .orderBy(F.col("balance").desc())
)
