from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

exploded = (
    flattened
    .select(
        "id",
        "customer_id",
        "key",
        "attr_value",
        F.explode("coupon_codes").alias("value"),
    )
)

with_dims = (
    exploded
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.country"), F.col("c.email"), F.col("c.signup_date"))
)

enriched = (
    with_dims
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .filter(F.length(F.col("c.name")) < 10)
    .select(
        "a.customer_id",
        "a.email",
        "a.signup_date",
        F.col("c.country").alias("customer_country"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        "s.customer_id",
        "s.email",
        "s.signup_date",
        "s.customer_country",
        F.col("c.signup_date").alias("customer_signup_date"),
        F.col("c.name"),
        F.col("c.email").alias("customer_email"),
    )
)

result = (
    joined
    .withColumn("year_start", F.date_trunc("year", "customer_signup_date"))
    .cube("year_start")
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("customer_country") == "TH", 1)).alias("th_count"),
        F.count(F.when(F.col("customer_country") == "US", 1)).alias("us_rows"),
        F.countDistinct("customer_country").alias("distinct_customer_countries"),
        F.count("*").alias("row_count"),
        F.grouping("year_start").alias("gid"),
    )
    .orderBy("th_count")
)
