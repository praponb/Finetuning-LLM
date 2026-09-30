from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "elem"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        (F.col("pos") + 1).alias("item_no"),
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

joined = (
    lines
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.name").like("K%"))
    .select("src.*", F.col("d.signup_date"), F.col("d.age"), F.col("d.country"))
)

enriched = (
    joined
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select(
        "a.*",
        F.col("d.country").alias("customer_country"),
        F.col("d.name"),
        F.col("d.signup_date").alias("customer_signup_date"),
    )
)

matched_keys = (
    enriched
    .select("id")
    .intersect(
        df_carts
        .filter(F.col("created_at") >= "2024-04-01")
        .select("id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined_carts = (
    reconciled
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.coupon_codes",
        "e.item_no",
        "e.product_id",
        "e.qty",
        "e.promo",
        "e.signup_date",
        "e.country",
        "e.customer_country",
        F.col("c.segment"),
        F.col("c.name").alias("customer_name"),
    )
)

result = (
    joined_carts
    .select(
        "customer_id",
        "item_no",
        "product_id",
        "promo",
        "country",
        "customer_country",
    )
    .orderBy(F.col("item_no").desc_nulls_first())
)
