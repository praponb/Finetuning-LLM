from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

exploded_carts = (
    exploded
    .select(
        "id",
        "customer_id",
        "attr_key",
        "attr_value",
        F.explode("coupon_codes").alias("code"),
    )
)

with_dims = (
    exploded_carts
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.customer_id",
        "src.attr_key",
        F.col("c.segment"),
        F.col("c.email"),
        F.col("c.signup_date"),
    )
)

with_dims_carts = (
    with_dims
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.age") >= 64)
    .select("s.*", F.col("p.age"), F.col("p.email").alias("customer_email"))
)

result = (
    with_dims_carts
    .select("customer_id", "attr_key", "age", "customer_email")
    .orderBy(F.col("attr_key").desc())
)
