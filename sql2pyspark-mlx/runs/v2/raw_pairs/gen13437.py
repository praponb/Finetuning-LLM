from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.discount") < 0.25)
    .groupBy(
        F.col("c.is_active"),
        F.month(F.col("c.signup_date")).alias("signup_month"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("o.status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
    )
    .orderBy(F.col("pct_cancelled").desc(), "is_active", "signup_month")
    .limit(5)
)
