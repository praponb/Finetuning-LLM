from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

exploded = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

result = (
    exploded
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "product_id",
        "promo",
        F.when(F.col("promo").rlike("^(VIP|WELCOME5)$"), "GROUP_A").otherwise("PRIORITY").alias("promo_zone"),
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy(F.col("customer_id").desc())
)
