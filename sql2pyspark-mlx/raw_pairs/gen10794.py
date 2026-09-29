from pyspark.sql import functions as F

matched_keys = (
    df_subscriptions
    .select("customer_id", "plan")
    .intersectAll(
        df_subscriptions
        .filter(F.month("end_date").isin([1, 5, 7]))
        .select("customer_id", "plan")
    )
)

verified = (
    df_subscriptions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
    )
    .orderBy(F.col("end_date").desc())
)
