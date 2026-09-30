from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "attr_value"),
    )
)

lines = (
    flattened
    .select("customer_id", "created_at", F.explode_outer("coupon_codes").alias("code"))
)

joined = (
    lines
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.signup_date"), F.col("p.age"))
)

joined_carts = (
    joined
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.age").alias("customer_age"))
)

summary = (
    joined_carts
    .groupBy("customer_id")
    .agg(
        F.max(F.when(F.col("code") == "BUNDLE", F.col("customer_age"))).alias("bundle_max_customer_age"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.max("created_at").alias("ended_at"),
    )
    .filter(F.col("bundle_max_customer_age") > 0)
)

result = (
    summary
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "bundle_max_customer_age",
        "ended_at",
        F.col("max_subscription_monthly_fee"),
    )
)
