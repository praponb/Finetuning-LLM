from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_semi")
    .select(F.col("c.id"), F.col("c.age"), F.col("c.country"), F.col("c.city"))
)
