from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

joined = (
    exploded
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.country") == "SG")
    .select(
        "a.id",
        "a.customer_id",
        "a.attr_value",
        F.col("b.credit_limit"),
        F.col("b.email"),
        F.col("b.name"),
    )
)

kept_keys = (
    joined
    .select("id", "customer_id")
    .intersect(
        df_carts
        .filter(F.to_date("created_at") >= "2025-01-22")
        .select("id", "customer_id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

matched_keys = (
    reconciled
    .select("id", "customer_id")
    .subtract(
        df_carts
        .filter(F.col("created_at") < "2025-06-01")
        .select("id", "customer_id")
    )
)

reconciled_carts = (
    reconciled
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

matched_keys_2 = (
    reconciled_carts
    .select("id")
    .intersect(
        df_carts
        .filter(F.col("customer_id").isin([22, 34]))
        .select("id")
    )
)

kept_rows = (
    reconciled_carts
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "a.*",
        F.col("d.credit_limit").alias("customer_credit_limit"),
        F.col("d.segment"),
    )
)

valid = (
    enriched
    .filter(F.col("segment").isNotNull() & (~F.col("segment").eqNullSafe(F.lit("SMB"))))
    .select(
        "id",
        "customer_id",
        "attr_value",
        "credit_limit",
        "email",
        "customer_credit_limit",
        F.col("customer_credit_limit").cast("int").alias("customer_credit_limit_int"),
    )
)

result = (
    valid
    .alias("src")
    .join(df_customers.alias("dim"), F.col("src.customer_id") == F.col("dim.id"), "inner")
    .rollup(F.col("dim.country").alias("country"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("src.customer_credit_limit_int")).alias("min_customer_credit_limit_int"),
        F.grouping_id().alias("gid"),
    )
)
