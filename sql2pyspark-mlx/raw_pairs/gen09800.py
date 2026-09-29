from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

matched_keys = (
    exploded
    .select("customer_id", "id")
    .intersect(
        df_carts
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id", "id")
    )
)

kept_rows = (
    exploded
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        F.col("b.age"),
        F.col("b.city"),
        F.col("b.country"),
    )
)

joined_carts = (
    joined
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.country",
        F.col("d.city").alias("customer_city"),
        F.col("d.signup_date"),
    )
)

enriched = (
    joined_carts
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .filter(F.col("c.segment").isin(["Corporate", "Consumer"]))
    .select(
        "s.id",
        "s.customer_id",
        "s.country",
        F.col("c.country").alias("customer_country"),
    )
)

cleaned = (
    enriched
    .filter(F.col("customer_id").isNotNull() & (F.col("customer_country") != "TH"))
    .withColumn("country_group", F.when(F.col("country").isin(["US", "SG"]), "Core").otherwise("Other"))
    .withColumn("is_us", F.when(F.col("country") == "US", 1).otherwise(0))
    .withColumn("customer_country_clean", F.coalesce("customer_country", F.lit("DIRECT")))
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.customer_id",
        "s.customer_country",
        "s.country_group",
        "s.is_us",
        F.col("b.credit_limit"),
        F.col("b.name"),
        F.col("b.age"),
    )
)

daily = (
    with_dims
    .groupBy(
        F.when(F.col("credit_limit") >= 16700, "LARGE").otherwise("SMALL").alias("credit_limit_tier"),
    )
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.sort_array(F.collect_set("customer_country")).alias("customer_country_list"),
        F.percentile_approx("age", 0.95).alias("age_p95"),
        F.size(F.collect_set("customer_country")).alias("num_customer_countries"),
    )
)

result = (
    daily
    .select("credit_limit_tier", "customer_country_list", "num_customer_countries")
    .orderBy("num_customer_countries", "credit_limit_tier")
)
