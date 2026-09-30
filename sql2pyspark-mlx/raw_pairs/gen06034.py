from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.min("started_at").alias("first_started_at"))
)

result = (
    df_customers.alias("customers")
    .join(customer_totals.alias("CustomerTotals"), F.col("customers.id") == F.col("CustomerTotals.customer_id"), "inner")
    .select(
        F.col("customers.id"),
        F.col("customers.signup_date"),
        F.col("CustomerTotals.first_started_at"),
    )
    .orderBy("signup_date", "id")
    .limit(3)
)
