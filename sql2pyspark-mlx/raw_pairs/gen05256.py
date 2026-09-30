from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.promo"),
    )
)

base = (
    exploded
    .filter(F.col("promo").isNotNull())
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "product_id",
        F.substring("promo", 1, 4).alias("promo_prefix"),
        F.coalesce("promo", F.lit("Unknown")).alias("promo_filled"),
        F.coalesce("promo", F.lit("NONE")).alias("promo_filled_2"),
    )
)

with_dims = (
    base
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.city"), F.col("b.age"), F.col("b.country"))
)

valid = (
    with_dims
    .filter(F.array_contains("coupon_codes", "WELCOME5"))
    .select(
        "*",
        F.array_contains("coupon_codes", "BUNDLE").alias("is_bundle"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

cleaned = (
    valid
    .filter(F.col("product_id") >= 6)
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "promo_prefix",
        "age",
        "country",
        "is_bundle",
        "city_clean",
        F.when(F.col("is_bundle"), "Yes").otherwise("No").alias("is_bundle_label"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

base_carts = (
    cleaned
    .filter(F.array_contains("coupon_codes", "VIP"))
    .select(
        "*",
        F.sort_array("coupon_codes").alias("sorted_coupon_codes"),
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)

by_type = (
    base_carts
    .groupBy("customer_id")
    .pivot("country", ["UK", "US", "SG"])
    .agg(F.sum("age"))
)

result = (
    by_type
    .select("customer_id", "UK", "US")
    .orderBy("UK", "US")
)
