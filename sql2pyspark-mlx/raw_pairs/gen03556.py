from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

exploded = (
    flattened
    .select(
        "id",
        "customer_id",
        "created_at",
        "k",
        "attr_value",
        F.explode("coupon_codes").alias("element"),
    )
)

remaining_keys = (
    exploded
    .select("customer_id")
    .intersect(
        df_carts
        .filter(F.col("created_at") >= "2025-07-01")
        .select("customer_id")
    )
)

reconciled = (
    exploded
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.is_active"))
    .select("e.customer_id", "e.created_at", "e.element", F.col("b.age"))
)

result = (
    enriched
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.country").alias("country"), F.col("d.city").alias("city"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.age")).alias("max_age"),
    )
    .orderBy(F.col("max_age").desc())
)
