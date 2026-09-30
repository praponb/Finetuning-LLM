from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("city").orderBy("subscription_monthly_fee_2", "id", "subscription_id", "subscription_id_2")
w3 = Window.partitionBy("plan").orderBy("subscription_id", "id", "subscription_id", "subscription_id_2")
w4 = Window.partitionBy("subscription_plan").orderBy("subscription_monthly_fee_2", "id", "subscription_id", "subscription_id_2")

valid = (
    df_subscriptions
    .filter(F.col("start_date") >= F.lit("2023-10-23").cast("date"))
    .select(
        "*",
        F.initcap(F.trim("plan")).alias("plan_title"),
        F.date_format("start_date", "MMM yyyy").alias("start_date_label"),
    )
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

joined = (
    ranked
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.city"), F.col("d.segment"))
)

with_window = (
    joined
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.start_date") >= F.col("sub.start_date")) & (F.col("a.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

attributed = (
    with_window
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.end_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan_2"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee_2"),
    )
)

joined_subscriptions = (
    attributed
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.signup_date"), F.col("b.age"), F.col("b.country"))
)

scored = (
    joined_subscriptions
    .select(
        "*",
        F.lag("subscription_id", 1).over(w2).alias("prev_subscription_id"),
        F.lag("subscription_id", 1).over(w3).alias("prev_subscription_id_2"),
        F.lag("subscription_monthly_fee_2", 1).over(w4).alias("prev_subscription_monthly_fee_2"),
    )
)

result = (
    scored
    .cube(
        F.when(F.col("age") >= 73, "LARGE").otherwise("SMALL").alias("age_tier"),
        "segment",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("age_per_singapore"),
        F.countDistinct("city").alias("distinct_cities"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
    )
    .filter(F.col("cnt") > 1)
)
