from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

valid = (
    flattened
    .filter((F.col("created_at") > F.lit("2025-03-02").cast("date")) & F.col("value").isNotNull())
    .withColumn("created_at_label", F.date_format("created_at", "MMM yyyy"))
)

enriched = (
    valid
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select(
        "s.customer_id",
        "s.created_at",
        "s.created_at_label",
        F.col("p.name"),
        F.col("p.segment"),
        F.col("p.credit_limit"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.segment") == "Consumer")
    .select("s.*", F.col("b.signup_date"))
)

result = (
    joined
    .select("created_at", "created_at_label", "segment", "credit_limit", "signup_date")
)
