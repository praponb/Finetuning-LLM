from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("monthly_fee").desc_nulls_first())
w2 = Window.partitionBy("customer_id", "status")
w3 = Window.partitionBy(F.year("start_date"), "status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_cume_dist").desc_nulls_last())

with_windows = (
    df_subscriptions
    .withColumn("monthly_fee_cume_dist", F.cume_dist().over(w))
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4))
    .withColumn("first_known_monthly_fee", F.first("monthly_fee", ignorenulls=True).over(w3))
)

ranked = (
    with_windows
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
)

attributed = (
    ranked
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.end_date") >= F.col("sub.start_date")) & (F.col("src.end_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

filtered = (
    attributed
    .filter(F.col("end_date") >= F.lit("2023-10-09").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        "monthly_fee_cume_dist",
        "monthly_fee_share",
        "first_known_monthly_fee",
        "row_num",
        "subscription_id",
        "subscription_plan",
        "subscription_monthly_fee",
        F.when(F.col("status") == "PAUSED", 1).otherwise(0).alias("is_paused"),
        F.coalesce("subscription_plan", F.lit("DIRECT")).alias("subscription_plan_filled"),
    )
)

with_dims = (
    filtered
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.plan",
        "a.monthly_fee",
        "a.status",
        "a.monthly_fee_cume_dist",
        "a.monthly_fee_share",
        "a.first_known_monthly_fee",
        "a.subscription_id",
        "a.subscription_plan",
        "a.is_paused",
        "a.subscription_plan_filled",
        F.col("b.credit_limit"),
        F.col("b.country"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "monthly_fee",
        "status",
        "monthly_fee_cume_dist",
        "monthly_fee_share",
        "first_known_monthly_fee",
        "subscription_id",
        "subscription_plan",
        "is_paused",
    )
)
