from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

valid = (
    lines
    .filter(F.col("element").isNotNull() & F.col("customer_id").isNull())
    .select(
        "*",
        F.date_trunc("WEEK", "created_at").alias("created_at_period"),
        F.coalesce("element", F.lit("UNKNOWN")).alias("resolved_element"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.is_active"))
    .select("src.customer_id", "src.created_at", "src.element", F.col("p.segment"))
)

enriched = (
    joined
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.credit_limit"), F.col("c.city"), F.col("c.signup_date"))
)

result = (
    enriched
    .groupBy(
        "customer_id",
        F.when(F.col("credit_limit") >= 12500, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
        "city",
    )
    .agg(
        F.min("created_at").alias("first_seen"),
        F.max("created_at").alias("ended_at"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.countDistinct("segment").alias("distinct_segments"),
        F.count("*").alias("row_count"),
    )
)
