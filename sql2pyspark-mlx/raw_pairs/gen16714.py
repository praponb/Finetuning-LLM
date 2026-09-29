from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("start_date") > F.to_date(F.lit("2023-10-02")))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.concat("status", F.lit("/"), "plan").alias("status_plan"),
        F.length("status").alias("status_length"),
    )
)

result = (
    base
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.end_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)
