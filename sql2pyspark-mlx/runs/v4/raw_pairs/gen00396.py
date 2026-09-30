from pyspark.sql import functions as F

matched_keys = (
    df_subscriptions
    .select("plan", "status")
    .exceptAll(
        df_subscriptions
        .filter(F.col("plan") == "basic")
        .select("plan", "status")
    )
)

kept_rows = (
    df_subscriptions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.city"), F.col("d.name"))
)

joined = (
    enriched
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.country"), F.col("p.email"))
)

result = (
    joined
    .select(
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        "city",
        "country",
        "email",
    )
    .orderBy(F.col("plan").desc_nulls_last())
)
