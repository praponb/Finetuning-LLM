from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.filter(F.col("balance").between(43000, 70000)).alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_anti")
    .filter(~F.col("c.email").like("%@GMAIL.COM"))
    .select(F.col("c.id"), F.col("c.email"), F.col("c.credit_limit"))
)
