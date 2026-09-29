from pyspark.sql import functions as F

flattened = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("element"))

enriched = (
    flattened
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.segment") == "SMB")
    .select("src.*", F.col("p.signup_date"))
)

enriched_carts = (
    enriched
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.element",
        "e.signup_date",
        F.col("p.city"),
        F.col("p.credit_limit"),
        F.col("p.email"),
    )
)

joined = (
    enriched_carts
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select(
        "s.customer_id",
        "s.element",
        "s.signup_date",
        "s.credit_limit",
        "s.email",
        F.col("p.country"),
        F.col("p.name"),
        F.col("p.segment"),
    )
)

joined_carts = (
    joined
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.customer_id", "src.signup_date", "src.credit_limit", F.col("c.age"))
)

summary = (
    joined_carts
    .rollup("customer_id")
    .agg(
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.count("*").alias("cnt"),
        F.min("credit_limit").alias("min_credit_limit"),
        F.percentile_approx("age", 0.95).alias("age_p95"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit_2"),
    )
)

result = (
    summary
    .select(
        "customer_id",
        "avg_credit_limit",
        "cnt",
        "min_credit_limit",
        "age_p95",
        "distinct_customer_ids",
        "avg_credit_limit_2",
    )
    .orderBy(F.col("avg_credit_limit").desc_nulls_first())
)
