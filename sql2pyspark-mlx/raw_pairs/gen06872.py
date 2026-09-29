from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

flattened = (
    exploded
    .select(
        "id",
        "created_at",
        "attr_key",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

valid = (
    flattened
    .filter(F.col("created_at") > F.lit("2025-03-01").cast("date"))
    .select(
        "*",
        F.date_format("created_at", "yyyy-MM-dd").alias("created_at_label"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_filled"),
    )
)

valid_carts = (
    valid
    .filter(F.col("attr_key") != "utm_source")
    .select(
        "*",
        F.date_format("created_at", "MMM yyyy").alias("created_at_label_2"),
        F.coalesce("attr_key", F.lit("NONE")).alias("attr_key_filled_2"),
    )
)

result = (
    valid_carts
    .select(
        "id",
        "created_at",
        "attr_key",
        "code",
        "attr_key_filled",
        "attr_key_filled_2",
    )
)
