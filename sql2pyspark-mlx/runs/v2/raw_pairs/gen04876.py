from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter((F.col("c.credit_limit") > 13400) & F.month(F.col("a.opened_date")).isin([3, 4, 9]))
    .select(F.col("c.name"), F.col("c.email"), F.col("a.balance"))
    .orderBy(F.col("balance").desc())
)
