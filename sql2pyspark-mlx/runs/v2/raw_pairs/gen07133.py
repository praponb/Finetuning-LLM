from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"), F.col("c.segment"))
    .agg(F.max(F.col("a.opened_date")).alias("last_opened_date"))
)
