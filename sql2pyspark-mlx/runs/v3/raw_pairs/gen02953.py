from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
        F.col("c.is_active"),
    )
    .agg(F.max(F.col("a.opened_date")).alias("latest_opened_date"))
    .orderBy(F.col("signup_month").desc())
)
