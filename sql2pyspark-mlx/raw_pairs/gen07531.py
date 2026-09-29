from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

lines = flattened.select("customer_id", "v", F.explode("coupon_codes").alias("element"))

enriched = (
    lines
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age").between(59, 64))
    .select("src.customer_id", "src.v", F.col("c.signup_date"), F.col("c.country"))
)

joined = (
    enriched
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select(
        "src.*",
        F.col("p.signup_date").alias("customer_signup_date"),
        F.col("p.country").alias("customer_country"),
    )
)

enriched_carts = (
    joined
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select(
        "e.customer_id",
        "e.v",
        "e.signup_date",
        "e.customer_country",
        F.col("d.age"),
        F.col("d.credit_limit"),
        F.col("d.country").alias("customer_country_2"),
    )
)

totals = (
    enriched_carts
    .groupBy("customer_country_2")
    .agg(
        F.max(F.when(F.col("customer_country") == "TH", F.col("credit_limit"))).alias("th_max_credit_limit"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("customer_country_2") == "JP", 1)), F.lit(0)), 2).alias("age_per_jp"),
        F.count("*").alias("cnt"),
    )
)

result = (
    totals
    .select("customer_country_2", "th_max_credit_limit", "cnt")
    .orderBy(F.col("th_max_credit_limit").desc_nulls_last())
)
