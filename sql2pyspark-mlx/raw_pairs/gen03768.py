from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "customer_id", F.explode_outer("coupon_codes").alias("element"))
)

joined = (
    lines
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.segment"), F.col("c.city"))
)

cleaned = (
    joined
    .filter(F.col("segment") == "Consumer")
    .select(
        "id",
        "customer_id",
        "segment",
        F.concat_ws(",", "city", "element").alias("city_element_key"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.age"))
)

enriched = (
    with_dims
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.city_element_key",
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
    )
)

result = (
    enriched
    .withColumn("quarter_start", F.date_trunc("QUARTER", "signup_date"))
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 13100, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["quarter_start", "credit_limit_tier"], ["quarter_start"], []],
        "quarter_start",
        "credit_limit_tier",
    )
    .agg(
        F.percentile_approx("credit_limit", 0.5).alias("credit_limit_p50"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count("*").alias("row_count"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.max("credit_limit").alias("max_credit_limit"),
        F.max("credit_limit").alias("max_credit_limit_2"),
    )
)
