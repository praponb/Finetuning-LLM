from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

lines = (
    flattened
    .select(
        "id",
        "created_at",
        "key",
        "value",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

cleaned = (
    lines
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "created_at",
        "key",
        "code",
        F.concat_ws(",", "key", "code").alias("key_code_key"),
        F.substring("key", 1, 2).alias("key_prefix"),
    )
)

base = (
    cleaned
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-02"))) & F.col("key").isNotNull())
    .select("*", F.substring("code", 1, 4).alias("code_prefix"))
)

base_carts = (
    base
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & (~F.col("key").eqNullSafe(F.lit("gift"))))
    .select(
        "*",
        F.dayofweek("created_at").alias("created_at_dayofweek"),
        F.coalesce("key", F.lit("NONE")).alias("key_clean"),
    )
)

cleaned_carts = (
    base_carts
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("key").isNotNull())
    .select(
        "id",
        "code",
        "key_code_key",
        "key_prefix",
        "created_at_dayofweek",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
        F.to_date("created_at").alias("created_at_date"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_clean_2"),
    )
)

result = (
    cleaned_carts
    .select(
        "id",
        "key_code_key",
        "key_prefix",
        "created_at_dayofweek",
        "created_at_date",
    )
    .orderBy("created_at_date")
)
