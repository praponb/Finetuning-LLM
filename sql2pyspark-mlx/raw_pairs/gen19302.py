from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-07-15").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.round((F.col("monthly_fee") * 1.07), 2).alias("adjusted_monthly_fee"),
    )
)

result = (
    valid
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.plan",
        "a.status",
        "a.adjusted_monthly_fee",
        F.col("p.age"),
        F.col("p.email"),
    )
)
