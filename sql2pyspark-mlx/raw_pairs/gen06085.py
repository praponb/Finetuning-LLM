from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-03-14"))) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        (F.col("credit_limit") + F.col("age")).alias("credit_limit_plus_age"),
    )
)

valid = (
    base
    .filter(F.col("signup_date") >= F.lit("2023-05-17").cast("date"))
    .select(
        "*",
        F.round((F.col("age") / F.col("credit_limit")), 2).alias("age_per_credit_limit"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.plan") == "standard"), "left_anti")
)

remaining_keys = (
    eligible
    .select("country")
    .intersect(
        df_customers
        .filter(F.col("country").isin(["JP", "US", "UK"]))
        .select("country")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("city")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("city")).alias("city_list_2"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.bit_or("age").alias("age_bit_or"),
    )
)
