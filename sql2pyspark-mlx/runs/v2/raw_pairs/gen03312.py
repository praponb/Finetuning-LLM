from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.filter(F.year("opened_date") == 2023).alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_semi")
    .filter(F.col("c.name").like("%Jac%"))
    .select(F.col("c.id"), F.col("c.email"), F.col("c.country"))
    .orderBy("email")
)
