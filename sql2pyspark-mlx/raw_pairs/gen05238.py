from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

kept_keys = (
    df_sessions
    .select("country")
    .intersectAll(
        df_sessions
        .filter(F.col("device").isin(["tablet", "desktop"]))
        .select("country")
    )
)

kept_rows = (
    df_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy(F.col("started_at").desc_nulls_last(), F.col("id").desc())
)
