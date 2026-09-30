from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("key", "value"),
    )
)

result = (
    lines
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "key",
        "value",
        F.when(F.col("key").rlike("^(weight_kg|utm_source)$"), "PRIORITY").when(F.col("key").rlike("^(device|gift)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("key_zone"),
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
