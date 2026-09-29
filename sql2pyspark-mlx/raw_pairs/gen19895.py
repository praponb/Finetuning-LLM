from pyspark.sql import functions as F

flattened = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("tag"))

filtered = (
    flattened
    .filter(F.col("customer_id").isin([8, 11, 30, 36]))
    .select("*", F.substring("tag", 1, 2).alias("tag_prefix"))
)

cleaned = (
    filtered
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.upper("tag").alias("tag_upper"),
        F.upper("tag").alias("tag_upper_2"),
        F.coalesce("tag", F.lit("NONE")).alias("resolved_tag"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.segment").isNotNull())
    .select("s.*", F.col("c.signup_date"), F.col("c.segment"))
)

joined = (
    with_dims
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.name"), F.col("b.email"))
)

result = (
    joined
    .select(
        "id",
        "customer_id",
        "tag",
        "tag_prefix",
        "tag_upper",
        "tag_upper_2",
        "signup_date",
        "segment",
        "email",
    )
)
