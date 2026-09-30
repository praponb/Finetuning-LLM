from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

with_dims = (
    flattened
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .filter(F.col("c.is_active"))
    .select("src.*", F.col("c.credit_limit"), F.col("c.age"))
)

enriched = (
    with_dims
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.credit_limit").alias("customer_credit_limit"),
        F.col("p.segment"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select(
        "s.customer_id",
        "s.credit_limit",
        "s.age",
        "s.customer_credit_limit",
        F.col("p.segment").alias("customer_segment"),
        F.col("p.country"),
    )
)

summary = (
    joined
    .groupBy("country")
    .agg(
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.count(F.when(F.col("customer_segment") == "Consumer", 1)).alias("consumer_rows"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("customer_segment") == "SMB", 1)).alias("smb_rows"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
    )
)

result = (
    summary
    .select("country", "sg_count", "cnt")
    .orderBy(F.col("country").desc())
)
