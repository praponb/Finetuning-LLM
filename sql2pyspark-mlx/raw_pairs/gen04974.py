from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("signup_date"), "city").orderBy(F.col("age").asc_nulls_last())
w_recent = Window.partitionBy("segment").orderBy(F.col("age").desc_nulls_first())
w3 = Window.partitionBy("city", "segment")
w_recent_2 = Window.partitionBy("city").orderBy("least_age_credit_limit")
w5 = Window.partitionBy("activity_month").orderBy(F.col("singapore_total_city_stddev_samp_age").desc())

matched_keys = (
    df_customers
    .select("segment", "id")
    .exceptAll(
        df_customers
        .filter(F.col("credit_limit") < 6900)
        .select("segment", "id")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys_2 = (
    reconciled
    .select("country", "city")
    .intersect(
        df_customers
        .filter(F.col("name").like("%Che%"))
        .select("country", "city")
    )
)

verified = (
    reconciled
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

matched_keys_3 = (
    verified
    .select("id", "country")
    .subtract(
        df_customers
        .filter(F.col("city").isin(["Singapore", "Chiang Mai", "Bangkok"]))
        .select("id", "country")
    )
)

reconciled_customers = (
    verified
    .alias("s")
    .join(matched_keys_3.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled_customers
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-06-02")))
    .withColumn("least_age_credit_limit", F.least("age", "credit_limit"))
    .withColumn("country_lower", F.lower("country"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("UNKNOWN")))
)

enriched = (
    cleaned
    .select(
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        "least_age_credit_limit",
        "country_lower",
        "resolved_city",
        F.percent_rank().over(w).alias("age_percent_rank"),
    )
)

scored = (
    enriched
    .filter(F.col("segment").isin(["Consumer", "SMB"]))
    .select(
        "*",
        F.percent_rank().over(w_recent).alias("age_percent_rank_2"),
        F.stddev_samp("age").over(w3).alias("city_stddev_samp_age"),
        F.rank().over(w_recent_2).alias("least_age_credit_limit_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.date_trunc("MONTH", "signup_date").alias("activity_month"),
        F.date_trunc("YEAR", "signup_date").alias("year_start"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("city_stddev_samp_age"))).alias("singapore_total_city_stddev_samp_age"),
        F.count("*").alias("event_count"),
        F.percentile_approx("city_stddev_samp_age", 0.9).alias("city_stddev_samp_age_p90"),
    )
    .filter(F.col("singapore_total_city_stddev_samp_age") > 0)
    .withColumn("singapore_total_city_stddev_samp_age_dense_rank", F.dense_rank().over(w5))
)
