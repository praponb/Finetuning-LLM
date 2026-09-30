from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy(F.col("credit_limit").desc_nulls_last())
w3 = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc_nulls_first())
w4 = Window.partitionBy("country")
w5 = Window.partitionBy("segment")

kept_keys = (
    df_customers
    .select("id")
    .exceptAll(
        df_customers
        .filter(F.col("name").like("He%"))
        .select("id")
    )
)

verified = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "*",
        F.corr("credit_limit", "age").over(w).alias("corr_credit_limit_age"),
        F.rank().over(w2).alias("credit_limit_rank"),
        F.dense_rank().over(w3).alias("credit_limit_dense_rank"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.count("corr_credit_limit_age").over(w4).alias("country_count_corr_credit_limit_age"),
        F.min("corr_credit_limit_age").over(w5).alias("segment_min_corr_credit_limit_age"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.country") == "US"), "left_anti")
)

valid = (
    eligible
    .filter((F.col("signup_date") > F.lit("2023-05-13").cast("date")) & F.col("city").isNotNull())
    .withColumn("country_title", F.initcap(F.trim("country")))
    .withColumn("city_clean", F.coalesce("city", F.lit("N/A")))
)

kept = (
    valid
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.country").isin(["DE", "UK", "JP"])), "left_anti")
)

daily = (
    kept
    .groupBy(
        F.date_trunc("YEAR", "signup_date").alias("year_start"),
        F.date_trunc("WEEK", "signup_date").alias("week_start"),
    )
    .agg(
        F.max(F.when(F.col("segment") == "SMB", F.col("country_count_corr_credit_limit_age"))).alias("smb_max_country_count_corr_credit_limit_age"),
        F.avg(F.when(F.col("country") == "UK", F.col("credit_limit_rank"))).alias("uk_avg_credit_limit_rank"),
        F.countDistinct("city").alias("distinct_cities"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
)

result = (
    daily
    .select(
        "year_start",
        "week_start",
        "smb_max_country_count_corr_credit_limit_age",
        "uk_avg_credit_limit_rank",
        "distinct_cities",
        "segment_list",
    )
)
