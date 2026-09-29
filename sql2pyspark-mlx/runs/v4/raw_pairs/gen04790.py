from pyspark.sql import functions as F

matched_keys = (
    df_subscriptions
    .select("customer_id")
    .intersectAll(
        df_subscriptions
        .filter(F.month("end_date").isin([3, 9, 11]))
        .select("customer_id")
    )
)

reconciled = (
    df_subscriptions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .select("e.*", F.col("c.city"), F.col("c.name"))
)

with_dims_subscriptions = (
    with_dims
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select(
        "src.*",
        F.col("b.signup_date"),
        F.col("b.email"),
        F.col("b.city").alias("customer_city"),
    )
)

remaining_keys = (
    with_dims_subscriptions
    .select("id", "plan")
    .intersect(
        df_subscriptions
        .filter(F.col("start_date").between("2023-12-01", "2025-01-01"))
        .select("id", "plan")
    )
)

kept_rows = (
    with_dims_subscriptions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

stats = (
    kept_rows
    .groupBy("status")
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
        F.count(F.when(F.col("customer_city") == "Singapore", 1)).alias("singapore_rows"),
    )
    .filter(F.col("cnt_2") < 1)
)

result = (
    stats
    .select(
        "status",
        "cnt_2",
        "singapore_rows",
        F.when(F.col("status").rlike("^(CANCELLED|EXPIRED)$"), "OTHER_ZONE").when(F.col("status").rlike("^(ACTIVE|PAUSED)$"), "EXTENDED").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("singapore_rows").asc_nulls_first())
)
