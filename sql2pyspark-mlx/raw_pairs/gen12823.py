from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "end_date"))
w2 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w3 = Window.partitionBy("status").orderBy("start_date", "id")
w4 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-2, -1)
w5 = Window.orderBy(F.col("subscription_monthly_fee").desc())
w_recent = Window.partitionBy("customer_id")
w7 = Window.partitionBy(F.month("end_date"), "customer_id")

metrics = (
    df_subscriptions
    .select(
        "*",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("seq_no"),
        F.avg("monthly_fee").over(w4).alias("monthly_fee_avg_2_prior"),
    )
)

with_window = (
    metrics
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.end_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

joined = (
    with_window
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.segment").isin(["SMB", "Consumer"]))
    .select(
        "s.id",
        "s.customer_id",
        "s.plan",
        "s.start_date",
        "s.end_date",
        "s.monthly_fee",
        "s.status",
        "s.monthly_fee_share",
        "s.row_seq",
        "s.subscription_id",
        "s.subscription_plan",
        "s.subscription_monthly_fee",
        F.col("c.segment"),
    )
)

matched_keys = (
    joined
    .select("status", "customer_id")
    .intersectAll(
        df_subscriptions
        .filter(F.month("start_date").isin([3, 7, 9]))
        .select("status", "customer_id")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.end_date",
        "a.monthly_fee_share",
        "a.subscription_monthly_fee",
        F.col("p.credit_limit"),
    )
)

metrics_subscriptions = (
    with_dims
    .select(
        "id",
        "monthly_fee_share",
        "subscription_monthly_fee",
        "credit_limit",
        F.cume_dist().over(w5).alias("subscription_monthly_fee_cume_dist"),
        F.avg("monthly_fee_share").over(w_recent).alias("customer_id_avg_monthly_fee_share"),
        F.max("subscription_monthly_fee").over(w7).alias("end_date_month_max_subscription_monthly_fee"),
    )
)

result = (
    metrics_subscriptions
    .select(
        "id",
        "monthly_fee_share",
        "subscription_monthly_fee",
        "credit_limit",
        "subscription_monthly_fee_cume_dist",
        "customer_id_avg_monthly_fee_share",
        "end_date_month_max_subscription_monthly_fee",
    )
    .orderBy(F.col("subscription_monthly_fee_cume_dist").desc())
)
