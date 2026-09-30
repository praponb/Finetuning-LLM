from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("started_at"), "device").orderBy("subscription_id", "id", "subscription_id")
w_ordered = Window.partitionBy(F.year("started_at"), "country").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(-5, Window.currentRow)
w4 = Window.orderBy(F.col("monthly_fee").desc(), "id", "subscription_id")

base = (
    df_sessions
    .filter((F.col("started_at") > F.to_timestamp(F.lit("2023-05-27 15:00:00"))) & F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.when(F.col("browser").isin(["Safari", "Edge"]), "Group A").otherwise("Other").alias("browser_group"),
        F.when(F.col("device").isin(["mobile", "desktop"]), "Tier 1").otherwise("Other").alias("device_group"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("browser_filled"),
    )
)

attributed = (
    base
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

enriched = (
    attributed
    .filter(F.col("monthly_fee").between(180, 220))
    .select(
        "*",
        F.ntile(10).over(w).alias("subscription_id_bucket"),
        F.last("browser", ignorenulls=True).over(w_ordered).alias("last_known_browser"),
        F.avg("monthly_fee").over(w3).alias("monthly_fee_avg_5"),
        F.ntile(5).over(w4).alias("monthly_fee_bucket"),
    )
)

enriched_sessions = (
    enriched
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .select(
        "s.*",
        F.col("b.country").alias("customer_country"),
        F.col("b.credit_limit"),
        F.col("b.city"),
    )
)

result = (
    enriched_sessions
    .cube(
        F.when(F.col("monthly_fee") >= 207, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        F.when(F.col("monthly_fee_bucket") >= 7, "HIGH").otherwise("LOW").alias("monthly_fee_bucket_tier"),
    )
    .agg(
        F.sum(F.when(F.col("customer_country") == "US", F.col("monthly_fee_avg_5"))).alias("us_total_monthly_fee_avg_5"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
        F.sum("subscription_id").alias("total_subscription_id"),
    )
    .filter(F.col("total_subscription_id") >= 0)
    .orderBy(F.col("quarter_start").desc())
)
