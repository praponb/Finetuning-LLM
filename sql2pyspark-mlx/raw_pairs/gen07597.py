from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

base = (
    lines
    .filter(F.size("coupon_codes") >= 2)
    .withColumn("sorted_coupon_codes", F.sort_array("coupon_codes"))
    .withColumn("resolved_k", F.coalesce("k", F.lit("N/A")))
)

enriched = (
    base
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.signup_date"), F.col("p.country"), F.col("p.email"))
)

enriched_carts = (
    enriched
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select(
        "s.customer_id",
        "s.created_at",
        "s.k",
        "s.sorted_coupon_codes",
        "s.resolved_k",
        "s.country",
        F.col("b.segment"),
        F.col("b.name"),
        F.col("b.email").alias("customer_email"),
    )
)

joined = (
    enriched_carts
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select(
        "src.customer_id",
        "src.created_at",
        "src.k",
        "src.country",
        "src.segment",
        "src.name",
        F.col("b.email"),
        F.col("b.credit_limit"),
        F.col("b.city"),
    )
)

result = (
    joined
    .groupBy("country")
    .agg(
        F.min("created_at").alias("first_created_at"),
        F.countDistinct("country").alias("distinct_countries"),
    )
    .orderBy(F.col("distinct_countries").asc_nulls_last())
)
