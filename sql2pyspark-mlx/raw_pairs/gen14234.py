from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

exploded = (
    lines
    .select(
        "id",
        "customer_id",
        "attr_key",
        "v",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

matched_keys = (
    exploded
    .select("id", "customer_id")
    .intersect(
        df_carts
        .filter(F.to_date("created_at") < "2025-02-26")
        .select("id", "customer_id")
    )
)

reconciled = (
    exploded
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .filter(F.col("d.credit_limit") <= 15700)
    .select(
        "src.id",
        "src.customer_id",
        "src.element",
        F.col("d.country"),
        F.col("d.email"),
    )
)

kept_keys = (
    with_dims
    .select("customer_id", "id")
    .subtract(
        df_carts
        .filter(F.to_date("created_at") >= "2024-10-19")
        .select("customer_id", "id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("element")
    .agg(
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("element") == "SPRING10", 1)).alias("spring10_rows"),
    )
    .filter(F.col("spring10_rows") >= 1)
)
