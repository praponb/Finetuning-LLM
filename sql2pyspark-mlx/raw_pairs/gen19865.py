from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-04-12").cast("date"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

valid = (
    cleaned
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-09-10")))
    .select(
        "*",
        F.upper("account_type").alias("account_type_upper"),
        F.date_add("opened_date", 23).alias("opened_date_plus_23d"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_filled"),
    )
)

valid_accounts = (
    valid
    .filter(F.col("balance").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "days_since_opened_date",
        "account_type_upper",
        "opened_date_plus_23d",
        "status_filled",
        (F.datediff("opened_date_plus_23d", "opened_date") / 7).alias("weeks_opened_date_to_opened_date_plus_23d"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_clean"),
    )
)

with_window = (
    valid_accounts
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.opened_date") >= F.col("sub.start_date")) & (F.col("e.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    with_window
    .select(
        "id",
        "account_type",
        "balance",
        "days_since_opened_date",
        "opened_date_plus_23d",
        "status_filled",
        "subscription_id",
        "monthly_fee",
    )
    .orderBy(F.col("opened_date_plus_23d").desc_nulls_first())
)
