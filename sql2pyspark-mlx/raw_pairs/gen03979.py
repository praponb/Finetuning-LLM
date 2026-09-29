from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

valid = (
    flattened
    .filter(F.col("created_at") >= F.lit("2025-03-01 22:00:00").cast("timestamp"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "key",
        "attr_value",
        F.sort_array("coupon_codes").alias("sorted_coupon_codes"),
        F.coalesce("key", F.lit("N/A")).alias("key_filled"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.created_at",
        "src.attr_value",
        F.col("b.country"),
    )
)

filtered = (
    joined
    .filter(F.col("created_at") > F.lit("2025-03-02 01:00:00").cast("timestamp"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "attr_value",
        "country",
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)

filtered_carts = (
    filtered
    .filter(F.col("country").eqNullSafe(F.lit("JP")))
    .select(
        "*",
        F.trim("country").alias("clean_country"),
        F.coalesce("country", F.lit("NONE")).alias("country_clean"),
    )
)

joined_carts = (
    filtered_carts
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.segment"))
)

enriched = (
    joined_carts
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.country").alias("customer_country"), F.col("b.signup_date"))
)

result = (
    enriched
    .select(
        "id",
        "created_at",
        "attr_value",
        "country",
        "country_filled",
        "clean_country",
        "customer_country",
        "signup_date",
        F.when(F.col("customer_country").rlike("^(TH|US)$"), "GROUP_B").when(F.col("customer_country").rlike("^(SG|JP)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("customer_country_zone"),
    )
    .orderBy(F.col("signup_date").desc())
)
