from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("code"))

enriched = (
    lines
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.signup_date") <= "2023-07-01")
    .select("a.*", F.col("b.age"))
)

valid = (
    enriched
    .filter(F.col("code") != "BUNDLE")
    .select(
        "*",
        F.when(F.col("age") >= 64, "High").when(F.col("age") >= 30, "Medium").otherwise("Low").alias("age_level"),
    )
)

joined = (
    valid
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select("a.customer_id", "a.code", F.col("c.age").alias("customer_age"))
)

enriched_carts = (
    joined
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.customer_id", "s.code", F.col("c.email"), F.col("c.segment"))
)

joined_carts = (
    enriched_carts
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.age"))
)

result = (
    joined_carts
    .groupBy(F.when(F.col("age") >= 69, "LARGE").otherwise("SMALL").alias("age_tier"))
    .agg(
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.sum(F.when(F.col("code") == "SPRING10", F.col("age")).otherwise(0)).alias("spring10_age"),
    )
    .filter(F.col("spring10_age") > 0)
    .orderBy(F.col("spring10_age").desc())
)
