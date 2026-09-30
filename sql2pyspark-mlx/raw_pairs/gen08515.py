from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "value"),
    )
)

kept_keys = (
    lines
    .select("id")
    .subtract(
        df_carts
        .filter(F.size("coupon_codes") >= 2)
        .select("id")
    )
)

verified = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-01"))) & (F.col("k").eqNullSafe(F.lit("weight_kg"))))
    .select(
        "created_at",
        "coupon_codes",
        "k",
        F.hour("created_at").alias("created_at_hour"),
        F.lower("k").alias("k_lower"),
    )
)

cleaned = (
    base
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & (F.col("created_at") >= "2025-01-01"))
    .select(
        "k",
        "k_lower",
        F.col("created_at_hour").cast("string").alias("created_at_hour_num"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_clean"),
    )
)

base_carts = (
    cleaned
    .filter(F.col("k").isin(["device", "utm_source", "weight_kg"]))
    .select(
        "*",
        F.substring("k", 1, 4).alias("k_prefix"),
        F.coalesce("k", F.lit("NONE")).alias("resolved_k"),
    )
)

result = (
    base_carts
    .select("k_lower", "created_at_hour_num", "k_clean", "k_prefix", "resolved_k")
)
