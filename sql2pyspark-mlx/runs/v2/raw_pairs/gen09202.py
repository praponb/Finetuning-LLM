from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.credit_limit") > 10500)
    .select(F.col("c.is_active"), F.col("a.balance"), F.col("a.opened_date"))
    .orderBy(F.col("balance").desc())
)
