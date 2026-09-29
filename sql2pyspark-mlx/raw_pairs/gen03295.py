from pyspark.sql import functions as F

matched_keys = (
    df_customers
    .select("country", "id")
    .intersect(
        df_customers
        .filter(F.col("country") == "TH")
        .select("country", "id")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("signup_date") > F.lit("2023-06-10").cast("date"))
    .withColumn("country_segment", F.concat("country", F.lit("/"), "segment"))
)

not_flagged = (
    base
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.end_date") > "2024-11-18"), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") >= F.col("s.signup_date")), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") < F.col("s.signup_date")), "left_semi")
)

totals = (
    kept
    .withColumn("activity_month", F.date_trunc("MONTH", "signup_date"))
    .withColumn("age_tier", F.when(F.col("age") >= 57, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["activity_month", "age_tier"], ["activity_month"], []],
        "activity_month",
        "age_tier",
    )
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_rows"),
        F.count("*").alias("event_count"),
        F.avg(F.when(F.col("country") == "UK", F.col("credit_limit"))).alias("uk_avg_credit_limit"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count("*").alias("cnt"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = totals.select("activity_month", "event_count", "country_list", "cnt", "grouping_id")
