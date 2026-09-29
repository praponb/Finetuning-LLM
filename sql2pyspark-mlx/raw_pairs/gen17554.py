from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("code"))
)

cleaned = (
    flattened
    .filter((F.col("created_at") > F.to_timestamp(F.lit("2025-03-01 15:00:00"))) & F.col("customer_id").isNotNull())
    .withColumn("code_upper", F.upper("code"))
)

filtered = (
    cleaned
    .filter((F.col("created_at") > F.to_timestamp(F.lit("2025-03-01 17:00:00"))) & (~F.col("code").eqNullSafe(F.lit("VIP"))))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.to_date("created_at").alias("created_at_date"),
    )
)

with_dims = (
    filtered
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.country"), F.col("c.signup_date"), F.col("c.segment"))
)

with_dims_carts = (
    with_dims
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.age") != 34)
    .select("s.*", F.col("b.signup_date").alias("customer_signup_date"))
)

result = (
    with_dims_carts
    .select(
        "id",
        "created_at",
        "created_at_date",
        "country",
        "signup_date",
        "customer_signup_date",
    )
    .orderBy(F.col("created_at").desc_nulls_last(), "signup_date")
)
