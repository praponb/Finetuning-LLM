from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date_plus_months", "id", "subscription_id").rowsBetween(-2, -1)
w2 = Window.partitionBy(F.date_trunc("month", "start_date_month_end"), "customer_id")
w3 = Window.orderBy("signup_date", "id", "subscription_id").rowsBetween(-5, -1)

filtered = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-03-22").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.last_day("start_date").alias("start_date_month_end"),
        F.add_months("start_date", 12).alias("start_date_plus_months"),
    )
)

filtered_subscriptions = (
    filtered
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.last_day("end_date").alias("end_date_month_end"),
        F.round("monthly_fee", 0).alias("monthly_fee_rounded"),
    )
)

enriched = (
    filtered_subscriptions
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.plan",
        "a.end_date",
        "a.monthly_fee",
        "a.status",
        "a.start_date_month_end",
        "a.start_date_plus_months",
        "a.end_date_month_end",
        "a.monthly_fee_rounded",
        F.col("p.signup_date"),
    )
)

attributed = (
    enriched
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.start_date_plus_months") >= F.col("sub.start_date")) & (F.col("src.start_date_plus_months") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

enriched_subscriptions = (
    attributed
    .filter(F.col("subscription_monthly_fee") <= 120)
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "start_date_month_end",
        "end_date_month_end",
        "signup_date",
        "subscription_id",
        "subscription_plan",
        "subscription_monthly_fee",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_2_prior"),
        F.max("monthly_fee").over(w2).alias("start_date_month_end_period_max_monthly_fee"),
        F.avg("subscription_id").over(w3).alias("subscription_id_avg_5_prior"),
    )
)

result = (
    enriched_subscriptions
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["segment", "city"], ["segment"]],
        F.col("p.segment").alias("segment"),
        F.col("p.city").alias("city"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("s.subscription_monthly_fee")).alias("avg_subscription_monthly_fee"),
        F.sum(F.col("s.subscription_id")).alias("total_subscription_id"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("segment").desc())
)
