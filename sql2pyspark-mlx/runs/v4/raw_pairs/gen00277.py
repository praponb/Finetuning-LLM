from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy("subscription_id", "id", "subscription_id")
w3 = Window.partitionBy("device").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.orderBy("monthly_fee", "id", "subscription_id").rowsBetween(-3, -1)
w5 = Window.partitionBy("plan").orderBy("is_safari", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-10-19").cast("date")) & F.col("browser").isNotNull())
    .withColumn("is_safari", F.when(F.col("browser") == "Safari", 1).otherwise(0))
    .withColumn("browser_device", F.concat("browser", F.lit(" - "), "device"))
)

attributed = (
    base
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.started_at") >= F.col("sub.start_date")) & (F.col("a.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

valid = (
    attributed
    .filter((F.col("started_at") > F.lit("2023-11-04 09:00:00").cast("timestamp")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "is_safari",
        "browser_device",
        "subscription_id",
        "plan",
        "monthly_fee",
        F.round("monthly_fee", 1).alias("monthly_fee_rounded"),
        F.coalesce("plan", F.lit("NONE")).alias("plan_clean"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.avg("monthly_fee_rounded").over(w).alias("customer_id_avg_monthly_fee_rounded"),
        F.ntile(4).over(w2).alias("subscription_id_bucket"),
        F.sum("is_safari").over(w3).alias("is_safari_sum_7d"),
        F.avg("subscription_id").over(w4).alias("subscription_id_avg_3_prior"),
        F.first("plan", ignorenulls=True).over(w5).alias("first_known_plan"),
    )
)

result = (
    enriched
    .select(
        "id",
        "subscription_id",
        "device",
        F.col("monthly_fee").alias("MONTHLY_FEE_VAL"),
        "monthly_fee_rounded",
    )
    .unpivot(
        ["id", "subscription_id", "device"],
        ["MONTHLY_FEE_VAL", "monthly_fee_rounded"],
        "measure",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
