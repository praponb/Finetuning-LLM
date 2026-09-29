from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "v"),
    )
)

lines_carts = (
    lines
    .select("customer_id", "created_at", "v", F.explode("coupon_codes").alias("code"))
)

valid = (
    lines_carts
    .filter(F.col("code").isNotNull())
    .select(
        "*",
        F.date_format("created_at", "MMM yyyy").alias("created_at_label"),
        F.coalesce("code", F.lit("OTHER")).alias("code_filled"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select(
        "s.customer_id",
        "s.created_at",
        "s.v",
        "s.code",
        "s.code_filled",
        F.col("d.name"),
    )
)

cleaned = (
    enriched
    .filter((F.col("created_at") > F.lit("2025-03-01 23:00:00").cast("timestamp")) & F.hour("created_at").between(9, 14))
    .select(
        "customer_id",
        "created_at",
        "code",
        "code_filled",
        "name",
        F.weekofyear("created_at").alias("created_at_weekofyear"),
        F.col("created_at").cast("date").alias("created_at_day"),
        F.coalesce("code", F.lit("DIRECT")).alias("resolved_code"),
    )
)

result = (
    cleaned
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "created_at",
        "code",
        "name",
        "created_at_day",
        F.when(F.col("name").rlike("^(Noah Park|Grace Kim)$"), "CORE").otherwise("OTHER_ZONE").alias("name_zone"),
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy(F.col("customer_id").desc_nulls_last(), F.col("code").desc())
)
