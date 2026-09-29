from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_totals.alias("customer_totals"), F.col("customers.id") == F.col("customer_totals.customer_id"), "inner")
    .select(
        F.col("customers.id"),
        F.col("customers.age"),
        F.col("customers.is_active"),
        F.col("customer_totals.plan_list"),
        F.col("customer_totals.monthly_fee_range"),
    )
)
