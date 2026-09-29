from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("status") == "CANCELLED", 1).otherwise(0).alias("is_cancelled"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.plan",
        "s.start_date",
        "s.is_cancelled",
        F.col("c.name"),
    )
)
