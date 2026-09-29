from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("account_type").orderBy("signup_date", "id")
w4 = Window.partitionBy(F.date_trunc("month", "subscription_end_date"), "country").orderBy(F.col("monthly_fee").desc(), "id")
w5 = Window.orderBy(F.col("monthly_fee").desc_nulls_last())

with_last = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.country",
        "s.segment",
        "s.signup_date",
        "s.is_active",
        "s.credit_limit",
        "s.age",
        F.col("c.end_date").alias("subscription_end_date"),
        F.col("c.monthly_fee"),
    )
)

with_last_customers = (
    with_last
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"), F.col("c.account_type"))
)

not_flagged = (
    with_last_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") < F.col("s.signup_date")), "left_anti")
)

metrics = (
    not_flagged
    .select(
        "id",
        "name",
        "country",
        "segment",
        "signup_date",
        "credit_limit",
        "subscription_end_date",
        "monthly_fee",
        "account_type",
        F.lead("age", 2).over(w3).alias("next_age"),
        F.row_number().over(w4).alias("row_seq"),
        F.percent_rank().over(w5).alias("monthly_fee_percent_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("monthly_fee") >= 212, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
    )
    .agg(
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.percentile_approx("monthly_fee_percent_rank", 0.5).alias("monthly_fee_percent_rank_p50"),
        F.round(F.avg("monthly_fee_percent_rank"), 2).alias("avg_monthly_fee_percent_rank"),
        F.max(F.when(F.col("country") == "UK", F.col("monthly_fee"))).alias("uk_max_monthly_fee"),
    )
    .orderBy("uk_max_monthly_fee")
)
