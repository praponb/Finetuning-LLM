from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").asc_nulls_first())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

enriched = (
    exploded
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("e.id", "e.customer_id", F.col("p.city"), F.col("p.segment"))
)

enriched_carts = (
    enriched
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.name").isNull())
    .select(
        "s.customer_id",
        "s.segment",
        F.col("b.email"),
        F.col("b.country"),
        F.col("b.city").alias("customer_city"),
    )
)

joined = (
    enriched_carts
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .select("src.*", F.col("c.age"), F.col("c.credit_limit"))
)

joined_carts = (
    joined
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "a.*",
        F.col("c.email").alias("customer_email"),
        F.col("c.age").alias("customer_age"),
        F.col("c.country").alias("customer_country"),
    )
)

filtered = (
    joined_carts
    .filter(F.col("customer_country").isNotNull() & (F.col("credit_limit") < 14800))
    .select(
        "customer_id",
        "segment",
        "email",
        "country",
        "age",
        "customer_age",
        "customer_country",
        F.upper("country").alias("country_upper"),
    )
)

base = (
    filtered
    .filter(F.col("segment").isNotNull())
    .select(
        "customer_id",
        "segment",
        "email",
        "country",
        "age",
        F.when(F.col("customer_age") >= 68, "High").when(F.col("customer_age") >= 32, "Medium").otherwise("Low").alias("customer_age_level"),
        F.coalesce("customer_country", F.lit("NONE")).alias("resolved_customer_country"),
    )
)

result = (
    base
    .select(
        "email",
        "country",
        "resolved_customer_country",
        F.cume_dist().over(w).alias("age_cume_dist"),
    )
)
