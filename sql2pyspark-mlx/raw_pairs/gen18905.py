from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("k", "v"),
    )
)

valid = (
    flattened
    .filter(F.col("k") == "gift")
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "k",
        F.array_join("coupon_codes", ", ").alias("coupon_codes_text"),
    )
)

joined = (
    valid
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.country") != "TH")
    .select("a.id", "a.customer_id", "a.k", "a.coupon_codes_text", F.col("b.segment"))
)

filtered = (
    joined
    .filter(F.col("segment") == "SMB")
    .select(
        "id",
        "customer_id",
        "k",
        "coupon_codes_text",
        "segment",
        F.when(F.col("k").isin(["device", "utm_source"]), "Priority").otherwise("Other").alias("k_group"),
        F.coalesce("segment", F.lit("DIRECT")).alias("segment_clean"),
    )
)

joined_carts = (
    filtered
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.k",
        "s.k_group",
        "s.segment_clean",
        F.col("b.city"),
        F.col("b.segment").alias("customer_segment"),
        F.col("b.country"),
    )
)

filtered_carts = (
    joined_carts
    .filter(F.col("customer_id").isNotNull() & (F.col("city").eqNullSafe(F.lit("Singapore"))))
    .select(
        "id",
        "customer_id",
        "k",
        "customer_segment",
        F.upper("city").alias("city_upper"),
        F.coalesce("customer_segment", F.lit("NONE")).alias("customer_segment_clean"),
    )
)

result = filtered_carts.select("id", "customer_id", "k", "customer_segment", "city_upper")
