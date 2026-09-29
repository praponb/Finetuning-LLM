from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "elem"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("position"),
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

exploded = (
    lines
    .select(
        "id",
        "customer_id",
        "created_at",
        "position",
        "product_id",
        "qty",
        "price",
        "promo",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

joined = (
    exploded
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select(
        "s.customer_id",
        "s.created_at",
        "s.position",
        "s.qty",
        "s.price",
        F.col("d.signup_date"),
        F.col("d.name"),
    )
)

joined_carts = (
    joined
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.country") == "JP")
    .select(
        "s.customer_id",
        "s.created_at",
        "s.position",
        "s.price",
        "s.name",
        F.col("c.signup_date").alias("customer_signup_date"),
    )
)

with_dims = (
    joined_carts
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "a.customer_id",
        "a.price",
        "a.name",
        F.col("d.age"),
        F.col("d.country"),
        F.col("d.name").alias("customer_name"),
    )
)

with_dims_carts = (
    with_dims
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select(
        "a.*",
        F.col("p.name").alias("customer_name_2"),
        F.col("p.age").alias("customer_age"),
        F.col("p.credit_limit"),
    )
)

enriched = (
    with_dims_carts
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.city"), F.col("d.age").alias("customer_age_2"))
)

result = (
    enriched
    .select(
        "customer_id",
        "price",
        "name",
        "age",
        "country",
        "customer_name",
        "credit_limit",
        "city",
        "customer_age_2",
    )
)
