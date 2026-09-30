from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(F.sum(F.when(F.col("a.status") == "OPEN", 1).otherwise(0)).alias("open_count"))
)
