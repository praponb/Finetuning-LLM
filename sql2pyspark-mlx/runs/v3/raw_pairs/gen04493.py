from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_frozen"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("c.signup_date"),
        F.col("t.pct_frozen"),
    )
)
