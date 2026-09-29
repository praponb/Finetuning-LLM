from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.filter(F.col("customer_id").isNull()).alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_anti")
    .filter(F.col("c.signup_date") < "2025-10-12")
    .select(F.col("c.id"), F.col("c.name"), F.col("c.city"))
)
