from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

with_dims = (
    flattened
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.city"), F.col("b.email"))
)

joined = (
    with_dims
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.signup_date"),
        F.col("p.email").alias("customer_email"),
        F.col("p.name"),
    )
)

enriched = (
    joined
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.country"))
)

result = (
    enriched
    .rollup("tag", F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.min("created_at").alias("first_seen"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .filter(F.col("cnt") < 1)
    .orderBy(F.col("activity_day").desc())
)
