from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy(F.year("opened_date"), "status").orderBy(F.col("balance").desc(), "id")
w_hist = Window.partitionBy(F.date_trunc("WEEK", "opened_date_plus_months"), "customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("credit_limit")

valid = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-08-18"))) & (F.col("opened_date") >= "2024-12-26"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.coalesce("status", F.lit("unassigned")).alias("status_filled"),
        F.add_months("opened_date", 12).alias("opened_date_plus_months"),
        F.coalesce("status", F.lit("OTHER")).alias("status_filled_2"),
    )
)

metrics = (
    valid
    .withColumn("balance_bucket", F.ntile(5).over(w_part))
    .withColumn("cumulative_balance", F.sum("balance").over(w_hist))
)

with_window = (
    metrics
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.opened_date_plus_months").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

joined = (
    with_window
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.age") >= 52)
    .select(
        "src.id",
        "src.customer_id",
        "src.account_type",
        "src.status",
        "src.status_filled_2",
        "src.cumulative_balance",
        "src.subscription_id",
        "src.plan",
        "src.monthly_fee",
        F.col("b.credit_limit"),
        F.col("b.country"),
    )
)

latest = (
    joined
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

with_dims = (
    latest
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.status",
        "s.subscription_id",
        "s.plan",
        "s.monthly_fee",
        F.col("b.segment"),
    )
)

not_flagged = (
    with_dims
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.is_flagged")), "left_anti")
)

stats = (
    not_flagged
    .groupBy(
        F.when(F.col("monthly_fee") >= 222, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.avg("subscription_id").alias("avg_subscription_id"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.size(F.collect_set("segment")).alias("num_segments_2"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("subscription_id")).otherwise(0)).alias("corporate_subscription_id"),
        F.round(F.avg("subscription_id"), 2).alias("avg_subscription_id_2"),
        F.percentile_approx("subscription_id", [0.25, 0.5, 0.75]).alias("subscription_id_quartiles"),
    )
)

result = (
    stats
    .select(
        "monthly_fee_tier",
        "avg_subscription_id",
        "num_segments",
        "corporate_subscription_id",
        "subscription_id_quartiles",
    )
)
