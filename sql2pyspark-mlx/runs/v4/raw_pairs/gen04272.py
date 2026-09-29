from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id")
w_part = Window.orderBy(F.col("start_date").desc(), "id")
w3 = Window.partitionBy("customer_id", "status").orderBy("monthly_fee_rounded", "id")

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        F.round("monthly_fee", 2).alias("monthly_fee_rounded"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.lead("monthly_fee", 2, 0).over(w).alias("next_monthly_fee"),
        F.row_number().over(w_part).alias("row_seq"),
        F.lag("monthly_fee", 2).over(w3).alias("prev_monthly_fee"),
    )
)

matched = (
    with_windows
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.start_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

with_dims = (
    matched
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.country"), F.col("b.name"))
)

result = (
    with_dims
    .groupBy("customer_id")
    .pivot("plan", ["basic", "enterprise"])
    .agg(F.max("monthly_fee"))
)
