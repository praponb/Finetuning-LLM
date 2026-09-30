from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-05-26").cast("date"))
    .withColumn("segment_title", F.initcap(F.trim("segment")))
)

remaining_keys = (
    base
    .select("segment", "city")
    .subtract(
        df_customers
        .filter(F.col("credit_limit") < 4500)
        .select("segment", "city")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter((F.col("signup_date") >= F.lit("2023-07-06").cast("date")) & F.col("city").isNotNull())
    .select(
        "*",
        (F.col("age") - F.col("credit_limit")).alias("age_minus_credit_limit"),
        F.trim("country").alias("clean_country"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.customer_id") == 8), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") <= F.col("s.signup_date")), "left_anti")
)

result = (
    not_flagged
    .groupBy("segment")
    .agg(
        F.bit_and("age").alias("age_bit_and"),
        F.sum(F.when(F.col("country") == "DE", F.col("credit_limit")).otherwise(0)).alias("de_credit_limit"),
        F.countDistinct("country").alias("distinct_countries"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
)
