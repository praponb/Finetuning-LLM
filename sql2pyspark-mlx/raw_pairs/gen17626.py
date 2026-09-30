from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("months_since_created_at").asc_nulls_first())
w2 = Window.orderBy(F.col("months_since_created_at").asc_nulls_last())
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "value"),
    )
)

filtered = (
    flattened
    .filter(~F.col("key").eqNullSafe(F.lit("utm_source")))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "value",
        (F.datediff(F.current_date(), "created_at") / 30).alias("months_since_created_at"),
        F.coalesce("key", F.lit("OTHER")).alias("key_filled"),
    )
)

remaining_keys = (
    filtered
    .select("id")
    .subtract(
        df_carts
        .filter(F.array_contains("coupon_codes", "SPRING10"))
        .select("id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "value",
        "months_since_created_at",
        "key_filled",
        F.rank().over(w).alias("months_since_created_at_rank"),
        F.dense_rank().over(w2).alias("months_since_created_at_dense_rank"),
        F.max("months_since_created_at").over(w3).alias("months_since_created_at_max_30m"),
        F.count("months_since_created_at").over(w4).alias("months_since_created_at_count_15m"),
    )
)

filtered_carts = (
    with_windows
    .filter(F.col("months_since_created_at_count_15m") >= 13)
    .select(
        "value",
        "months_since_created_at",
        "key_filled",
        "months_since_created_at_rank",
        "months_since_created_at_dense_rank",
        F.round((F.col("months_since_created_at_count_15m") * 0.85), 2).alias("months_since_created_at_count_15m_adjusted"),
        F.round((F.col("months_since_created_at") * 1.15), 2).alias("months_since_created_at_adjusted"),
    )
)

base = (
    filtered_carts
    .filter(F.col("months_since_created_at_adjusted").between(35, 74))
    .select(
        "*",
        (F.col("months_since_created_at") + F.col("months_since_created_at_rank")).alias("months_since_created_at_plus_months_since_created_at_rank"),
        (F.col("months_since_created_at_dense_rank") - F.col("months_since_created_at_count_15m_adjusted")).alias("months_since_created_at_dense_rank_minus_months_since_created_at_count_15m_adjusted"),
    )
)

valid = (
    base
    .filter(F.col("months_since_created_at_adjusted").between(74, 78))
    .select(
        "*",
        (F.col("months_since_created_at_rank") - F.col("months_since_created_at")).alias("months_since_created_at_rank_minus_months_since_created_at"),
    )
)

result = (
    valid
    .select(
        "value",
        "months_since_created_at",
        "months_since_created_at_rank",
        "months_since_created_at_dense_rank",
        "months_since_created_at_adjusted",
        "months_since_created_at_plus_months_since_created_at_rank",
        "months_since_created_at_rank_minus_months_since_created_at",
    )
)
