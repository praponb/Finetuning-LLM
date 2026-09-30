from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "right")
    .filter(F.month(F.col("c.signup_date")).isin([1, 7, 9]))
    .groupBy(F.col("c.city"), F.col("c.country"))
    .agg(F.countDistinct(F.col("s.browser")).alias("unique_browsers"))
    .filter(F.col("unique_browsers") > 3)
    .orderBy(F.col("unique_browsers").desc())
)
