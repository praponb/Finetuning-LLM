from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

lines = (
    flattened
    .select(
        "id",
        "customer_id",
        "product_id",
        "price",
        "promo",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

with_dims = (
    lines
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.age"), F.col("c.email"), F.col("c.city"))
)

matched_keys = (
    with_dims
    .select("customer_id")
    .intersect(
        df_carts
        .filter(F.col("created_at") < "2024-03-02")
        .select("customer_id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "product_id",
        "price",
        "element",
        "age",
        "city",
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("avg_subscription_monthly_fee").desc())
)
