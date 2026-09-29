from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee")).otherwise(0)).alias("basic_monthly_fee"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_totals.alias("customer_totals"), F.col("customers.id") == F.col("customer_totals.customer_id"), "left")
    .filter(F.col("customer_totals.basic_monthly_fee") > 57)
    .select(
        F.col("customers.id"),
        F.col("customers.email"),
        F.col("customer_totals.basic_monthly_fee"),
    )
    .orderBy(F.col("id").desc())
)
