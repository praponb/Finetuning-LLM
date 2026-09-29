from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(F.col("a.opened_date"), F.col("a.customer_id"), F.col("c.credit_limit"))
    .orderBy("opened_date")
)
