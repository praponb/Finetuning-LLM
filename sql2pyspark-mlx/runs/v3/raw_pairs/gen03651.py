from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.filter(F.col("credit_limit") < 13500).alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("a.opened_date") < "2025-09-01")
    .select(F.col("a.id"), F.col("a.opened_date"))
    .orderBy("id", F.col("opened_date").desc())
)
