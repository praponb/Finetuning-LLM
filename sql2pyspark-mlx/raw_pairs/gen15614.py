from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
    )
)

enriched = (
    lines
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.credit_limit") > 5800)
    .select(
        "e.id",
        "e.customer_id",
        "e.created_at",
        "e.coupon_codes",
        "e.pos",
        "e.product_id",
        "e.qty",
        "e.price",
        F.col("d.email"),
        F.col("d.city"),
    )
)

remaining_keys = (
    enriched
    .select("customer_id", "id")
    .subtract(
        df_carts
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id", "id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 10:00:00")))
    .select(
        "*",
        F.when(F.col("city").isin(["London", "Chiang Mai"]), "Core").otherwise("Other").alias("city_group"),
        (F.col("product_id") + F.col("pos")).alias("product_id_plus_pos"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.age"), F.col("c.credit_limit"))
)

with_dims = (
    joined
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select(
        "a.*",
        F.col("d.email").alias("customer_email"),
        F.col("d.city").alias("customer_city"),
    )
)

enriched_carts = (
    with_dims
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "s.customer_id",
        "s.coupon_codes",
        "s.product_id",
        "s.qty",
        "s.city",
        "s.city_group",
        "s.product_id_plus_pos",
        "s.age",
        "s.customer_city",
        F.col("d.name"),
    )
)

result = enriched_carts.select("product_id", "qty", "city", "name")
