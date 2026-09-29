from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("status", "plan").orderBy(F.col("end_date").desc(), "id")

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & F.col("customer_id").isNotNull())
    .select("*", F.trim("plan").alias("clean_plan"))
)

scored = (
    valid
    .select(
        "*",
        F.ntile(3).over(w).alias("monthly_fee_bucket"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

joined = (
    scored
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.age"), F.col("b.name"), F.col("b.email"))
)

attributed = (
    joined
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.start_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

result = (
    attributed
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .cube(F.col("c.city").alias("city"), F.col("c.country").alias("country"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.subscription_id")).alias("min_subscription_id"),
        F.min(F.col("s.monthly_fee_bucket")).alias("min_monthly_fee_bucket"),
        F.grouping_id().alias("level_id"),
    )
)
