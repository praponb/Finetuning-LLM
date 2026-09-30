from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

exploded = (
    lines
    .select(
        "id",
        "customer_id",
        "created_at",
        "key",
        "value",
        F.explode_outer("coupon_codes").alias("value_2"),
    )
)

kept_keys = (
    exploded
    .select("customer_id", "id")
    .intersect(
        df_carts
        .filter(F.array_contains("coupon_codes", "SPRING10"))
        .select("customer_id", "id")
    )
)

reconciled = (
    exploded
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.email").isNotNull())
    .select(
        "a.id",
        "a.customer_id",
        "a.created_at",
        "a.value",
        "a.value_2",
        F.col("p.city"),
        F.col("p.country"),
    )
)

grouped = (
    joined
    .groupBy("customer_id")
    .agg(
        F.min("created_at").alias("started_at"),
        F.size(F.collect_set("value_2")).alias("num_value_2s"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)

result = (
    grouped
    .select("customer_id", "started_at", "num_value_2s", "singapore_count")
    .orderBy(F.col("customer_id").desc(), "singapore_count")
    .limit(5)
)
