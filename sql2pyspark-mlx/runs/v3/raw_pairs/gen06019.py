from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.filter(F.col("opened_date") <= "2024-12-05").alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_anti")
    .select(F.col("c.id"), F.col("c.signup_date"))
    .orderBy(F.col("id").desc())
)
