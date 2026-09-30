from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("value"))
)

joined = (
    exploded
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select("s.*", F.col("c.signup_date"))
)

enriched = (
    joined
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select(
        "s.*",
        F.col("b.signup_date").alias("customer_signup_date"),
        F.col("b.credit_limit"),
    )
)

result = (
    enriched
    .withColumn("week_start", F.date_trunc("WEEK", "signup_date"))
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 12400, "LARGE").otherwise("SMALL"))
    .withColumn("year_start", F.date_trunc("YEAR", "signup_date"))
    .cube("week_start", "credit_limit_tier", "year_start")
    .agg(
        F.count_if(F.col("value") == "WELCOME5").alias("welcome5_events"),
        F.min("credit_limit").alias("min_credit_limit"),
        F.size(F.collect_set("value")).alias("num_values"),
        F.grouping_id().alias("grouping_level"),
    )
)
