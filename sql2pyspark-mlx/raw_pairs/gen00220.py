from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy(F.col("last3_max_subscription_monthly_fee").desc())

attributed = (
    df_subscriptions
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.start_date") >= F.col("sub.start_date")) & (F.col("s.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

scored = (
    attributed
    .filter(F.col("monthly_fee").between(80, 190))
    .withColumn("last3_count_subscription_monthly_fee", F.count("subscription_monthly_fee").over(w))
    .withColumn("last3_max_subscription_monthly_fee", F.max("subscription_monthly_fee").over(w))
    .withColumn("last3_min_subscription_monthly_fee", F.min("subscription_monthly_fee").over(w))
)

ranked = (
    scored
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

joined = (
    ranked
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("e.*", F.col("p.age"), F.col("p.credit_limit"))
)

base = (
    joined
    .filter((F.col("subscription_plan").eqNullSafe(F.lit("enterprise"))) & F.col("subscription_plan").isNotNull())
    .select(
        "*",
        (F.col("last3_max_subscription_monthly_fee") - F.col("last3_count_subscription_monthly_fee")).alias("last3_max_subscription_monthly_fee_minus_last3_count_subscription_monthly_fee"),
    )
)

remaining_keys = (
    base
    .select("status", "id")
    .intersect(
        df_subscriptions
        .filter(F.year("end_date") >= 2024)
        .select("status", "id")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "plan",
        "status",
        "subscription_id",
        "subscription_plan",
        "subscription_monthly_fee",
        "last3_count_subscription_monthly_fee",
        "last3_max_subscription_monthly_fee",
        "age",
        "last3_max_subscription_monthly_fee_minus_last3_count_subscription_monthly_fee",
    )
)
