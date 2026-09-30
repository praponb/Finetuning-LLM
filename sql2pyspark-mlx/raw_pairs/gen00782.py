from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

exploded_carts = (
    exploded
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

with_dims = (
    exploded_carts
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .filter(F.upper(F.col("p.name")).like("%ACK%"))
    .select("s.*", F.col("p.segment"))
)

remaining_keys = (
    with_dims
    .select("id", "customer_id")
    .subtract(
        df_carts
        .filter(F.col("created_at") < "2025-01-30")
        .select("id", "customer_id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter((F.col("created_at") >= F.lit("2025-03-01 12:00:00").cast("timestamp")) & F.col("segment").isNotNull())
    .select("*", F.lower("value").alias("value_lower"))
)

result = (
    base
    .select("id", "created_at", "value", "segment")
    .orderBy(F.col("created_at").desc_nulls_first())
)
