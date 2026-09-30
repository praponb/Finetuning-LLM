from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

valid = (
    exploded
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "k",
        "v",
        F.sort_array("coupon_codes").alias("sorted_coupon_codes"),
    )
)

valid_carts = (
    valid
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        F.sort_array("sorted_coupon_codes").alias("sorted_sorted_coupon_codes"),
        F.size("sorted_coupon_codes").alias("sorted_coupon_codes_count"),
    )
)

joined = (
    valid_carts
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .filter(F.col("p.city") != "London")
    .select("src.*", F.col("p.email"))
)

with_dims = (
    joined
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select("e.*", F.col("c.segment"))
)

enriched = (
    with_dims
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.city"),
        F.col("p.country"),
        F.col("p.segment").alias("customer_segment"),
    )
)

result = (
    enriched
    .groupBy(
        F.date_trunc("day", "created_at").alias("activity_day"),
        F.date_trunc("hour", "created_at").alias("hour_window"),
    )
    .agg(
        F.round(F.avg("sorted_coupon_codes_count"), 2).alias("avg_sorted_coupon_codes_count"),
        F.percentile_approx("sorted_coupon_codes_count", [0.25, 0.5, 0.75]).alias("sorted_coupon_codes_count_quartiles"),
        F.min("created_at").alias("started_at"),
        F.min("created_at").alias("first_created_at"),
        F.count_if(F.col("segment") == "Corporate").alias("corporate_events"),
    )
    .filter(F.col("avg_sorted_coupon_codes_count") < 1000000)
    .orderBy(F.col("corporate_events").desc_nulls_first())
)
