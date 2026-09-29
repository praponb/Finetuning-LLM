from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter(F.col("end_date") > F.to_date(F.lit("2024-02-05")))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
    )
)

result = (
    valid
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .select("e.*", F.col("c.signup_date"), F.col("c.segment"), F.col("c.name"))
)
