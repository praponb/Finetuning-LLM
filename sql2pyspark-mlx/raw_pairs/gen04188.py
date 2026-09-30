from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("WEEK", "customer_signup_date"))

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

exploded = (
    flattened
    .select(
        "id",
        "customer_id",
        "created_at",
        "key",
        "attr_value",
        F.explode("coupon_codes").alias("code"),
    )
)

enriched = (
    exploded
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "left")
    .filter(F.col("c.name").isNotNull())
    .select(
        "a.customer_id",
        "a.created_at",
        "a.key",
        "a.code",
        F.col("c.age"),
        F.col("c.country"),
        F.col("c.segment"),
    )
)

with_dims = (
    enriched
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select(
        "s.customer_id",
        "s.created_at",
        "s.key",
        "s.code",
        "s.country",
        F.col("b.age").alias("customer_age"),
        F.col("b.email"),
    )
)

enriched_carts = (
    with_dims
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .select("src.*", F.col("p.credit_limit"), F.col("p.name"), F.col("p.signup_date"))
)

enriched_carts_2 = (
    enriched_carts
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select(
        "e.*",
        F.col("c.credit_limit").alias("customer_credit_limit"),
        F.col("c.signup_date").alias("customer_signup_date"),
    )
)

scored = (
    enriched_carts_2
    .select(
        "customer_id",
        "key",
        "credit_limit",
        "name",
        "signup_date",
        F.max("customer_credit_limit").over(w).alias("customer_credit_limit_max_1h"),
        F.count("customer_age").over(w2).alias("customer_age_count_1h"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w3)), 4).alias("credit_limit_share"),
    )
)

result = (
    scored
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.city").alias("city"), F.col("src.key").alias("key"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("src.credit_limit")), 2).alias("total_credit_limit"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("key").desc_nulls_last())
)
