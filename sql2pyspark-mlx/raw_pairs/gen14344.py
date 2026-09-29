from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("subscription_plan")
w2 = Window.partitionBy("plan").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("customer_id", "subscription_plan").orderBy("start_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

cleaned = (
    df_subscriptions
    .filter(F.col("plan").isin(["premium", "standard"]))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
    )
)

cleaned_subscriptions = (
    cleaned
    .filter(F.col("customer_id").isNotNull() & (F.col("start_date") >= "2023-04-07"))
    .select("*", F.date_add("start_date", 72).alias("start_date_plus_72d"))
)

with_window = (
    cleaned_subscriptions
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.start_date_plus_72d") >= F.col("sub.start_date")) & (F.col("src.start_date_plus_72d") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

with_windows = (
    with_window
    .filter(F.col("end_date") >= "2025-02-01")
    .select(
        "*",
        F.round((F.col("subscription_id") / F.sum("subscription_id").over(w)), 4).alias("subscription_id_share"),
        F.sum("subscription_id").over(w2).alias("cumulative_subscription_id"),
        F.first("monthly_fee", ignorenulls=True).over(w_recent).alias("first_known_monthly_fee"),
    )
)

enriched = (
    with_windows
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.end_date",
        "src.monthly_fee",
        "src.status",
        "src.days_since_end_date",
        "src.start_date_plus_72d",
        "src.subscription_id",
        "src.subscription_plan",
        "src.subscription_monthly_fee",
        "src.subscription_id_share",
        "src.cumulative_subscription_id",
        "src.first_known_monthly_fee",
        F.col("p.signup_date"),
        F.col("p.email"),
    )
)

result = (
    enriched
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "status",
        "days_since_end_date",
        "start_date_plus_72d",
        "subscription_id",
        "subscription_plan",
        "cumulative_subscription_id",
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy(F.col("subscription_plan").asc_nulls_first())
)
