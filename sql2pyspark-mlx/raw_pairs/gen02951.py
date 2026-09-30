from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-08-02").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.datediff("start_date", "end_date").alias("days_end_date_to_start_date"),
        F.length("plan").alias("plan_length"),
    )
)

enriched = (
    filtered
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.customer_id",
        "s.days_end_date_to_start_date",
        "s.plan_length",
        F.col("d.credit_limit"),
        F.col("d.country"),
    )
)

remaining_keys = (
    enriched
    .select("id", "customer_id")
    .intersect(
        df_subscriptions
        .filter(F.col("monthly_fee").isNotNull())
        .select("id", "customer_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched_subscriptions = (
    reconciled
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.days_end_date_to_start_date",
        "src.credit_limit",
        F.col("b.country").alias("customer_country"),
        F.col("b.credit_limit").alias("customer_credit_limit"),
    )
)

enriched_subscriptions_2 = (
    enriched_subscriptions
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.signup_date") >= "2023-10-22")
    .select("e.*", F.col("c.segment"), F.col("c.city"))
)

with_dims = (
    enriched_subscriptions_2
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.days_end_date_to_start_date",
        "src.customer_country",
        "src.segment",
        "src.city",
        F.col("d.signup_date"),
        F.col("d.segment").alias("customer_segment"),
        F.col("d.age"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "customer_id",
        "segment",
        "city",
        F.when(F.col("city").rlike("^(Berlin|Chiang Mai)$"), "GROUP_A").when(F.col("city").rlike("^(Bangkok|New York)$"), "EXTENDED").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("city").desc())
)
