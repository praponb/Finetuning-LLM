from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.signup_date") <= "2024-09-18")
    .groupBy(
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"),
        F.col("c.segment"),
    )
    .agg(
        F.countDistinct(F.col("s.customer_id")).alias("unique_customers"),
        F.countDistinct(F.col("s.browser")).alias("unique_browsers"),
    )
    .filter(F.col("unique_customers") >= 4)
    .orderBy(F.col("unique_customers").desc())
)
