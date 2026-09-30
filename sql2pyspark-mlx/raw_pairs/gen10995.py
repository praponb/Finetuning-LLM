from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("plan") == "premium"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.concat("status", F.lit(" - "), "plan").alias("status_plan"),
    )
)

kept_keys = (
    cleaned
    .select("status", "customer_id")
    .intersect(
        df_subscriptions
        .filter(F.col("customer_id").isNotNull())
        .select("status", "customer_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.country"))
)

cleaned_subscriptions = (
    with_dims
    .filter(F.col("plan").isin(["basic", "premium"]))
    .select(
        "id",
        "customer_id",
        "start_date",
        "status",
        F.add_months("end_date", 12).alias("end_date_plus_months"),
        F.when(F.col("plan").isin(["basic", "premium"]), "Priority").otherwise("Other").alias("plan_group"),
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)

with_dims_subscriptions = (
    cleaned_subscriptions
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.customer_id",
        "e.status",
        "e.end_date_plus_months",
        "e.plan_group",
        F.col("p.country"),
    )
)

with_dims_subscriptions_2 = (
    with_dims_subscriptions
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.city").isin(["Tokyo", "Berlin"]))
    .select("s.*", F.col("b.segment"))
)

result = (
    with_dims_subscriptions_2
    .groupBy(F.date_trunc("quarter", "end_date_plus_months").alias("quarter_start"))
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.count_if(F.col("segment") == "Corporate").alias("corporate_events"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
    )
)
