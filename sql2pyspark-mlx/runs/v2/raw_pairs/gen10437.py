from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit").between(14500, 16100))
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"), F.col("c.city"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.plan") == "enterprise", 1).otherwise(0))) / F.count("*")), 1).alias("pct_enterprise"),
    )
    .orderBy(F.col("pct_enterprise").desc())
)
