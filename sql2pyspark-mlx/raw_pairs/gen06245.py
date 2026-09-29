from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-08-01 20:00:00").cast("timestamp")) & (~F.col("country").eqNullSafe(F.lit("JP"))))
    .withColumn("device_title", F.initcap(F.trim("device")))
)

valid = (
    filtered
    .filter((F.col("started_at") > F.to_timestamp(F.lit("2023-03-14 17:00:00"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "device_title",
        F.trim("browser").alias("clean_browser"),
        F.coalesce("country", F.lit("DIRECT")).alias("resolved_country"),
    )
)

enriched = (
    valid
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.segment"), F.col("b.signup_date"), F.col("b.credit_limit"))
)

gaps = (
    enriched
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "device_title",
        "clean_browser",
        "segment",
        "signup_date",
        F.col("max_subscription_monthly_fee"),
    )
    .orderBy("country")
)
