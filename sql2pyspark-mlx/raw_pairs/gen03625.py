from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

exploded = (
    flattened
    .select("id", "customer_id", "attr_key", F.explode("coupon_codes").alias("value"))
)

enriched = (
    exploded
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.segment").isin(["Consumer", "SMB"]))
    .select("a.customer_id", "a.attr_key", F.col("p.city"))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.name"))
)

enriched_carts = (
    with_dims
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select(
        "e.customer_id",
        "e.attr_key",
        F.col("d.credit_limit"),
        F.col("d.signup_date"),
    )
)

result = (
    enriched_carts
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "left")
    .groupBy(
        F.col("dim.segment").alias("segment"),
        F.col("dim.country").alias("country"),
        F.col("e.attr_key").alias("attr_key"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.credit_limit")).alias("max_credit_limit"),
    )
    .orderBy("country")
)
