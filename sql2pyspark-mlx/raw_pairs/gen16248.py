from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("a.status").isin(["CLOSED", "OPEN"]))
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(F.count("*").alias("row_count"))
)
