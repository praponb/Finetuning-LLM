from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status", "country")
w3 = Window.partitionBy(F.year("signup_date"), "country")
w4 = Window.partitionBy("segment")

filtered = (
    df_customers
    .filter(F.col("credit_limit").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        (F.col("credit_limit") + F.col("age")).alias("credit_limit_plus_age"),
        F.date_add("signup_date", 82).alias("signup_date_plus_82d"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled"),
    )
)

valid = (
    filtered
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-03-18"))) & F.col("city").isNotNull())
    .select(
        "*",
        F.lower("name").alias("name_lower"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
        F.coalesce("segment", F.lit("NONE")).alias("resolved_segment"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.status"),
        F.col("c.account_type").alias("account_account_type"),
    )
)

remaining_keys = (
    with_last
    .select("country", "city")
    .subtract(
        df_customers
        .filter(F.col("name").isNotNull())
        .select("country", "city")
    )
)

verified = (
    with_last
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "id",
        "is_active",
        "credit_limit_plus_age",
        "resolved_segment",
        "status",
        "account_account_type",
        F.min("credit_limit_plus_age").over(w2).alias("status_min_credit_limit_plus_age"),
        F.avg("credit_limit_plus_age").over(w3).alias("signup_date_year_avg_credit_limit_plus_age"),
        F.min("credit_limit_plus_age").over(w4).alias("segment_min_credit_limit_plus_age"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("signup_date_year_avg_credit_limit_plus_age") >= 70, "LARGE").otherwise("SMALL").alias("signup_date_year_avg_credit_limit_plus_age_tier"),
    )
    .agg(
        F.sum(F.when(F.col("account_account_type") == "CHECKING", F.col("signup_date_year_avg_credit_limit_plus_age"))).alias("checking_total_signup_date_year_avg_credit_limit_plus_age"),
        F.avg(F.when(F.col("account_account_type") == "CHECKING", F.col("segment_min_credit_limit_plus_age"))).alias("checking_avg_segment_min_credit_limit_plus_age"),
        F.percentile_approx("segment_min_credit_limit_plus_age", 0.5).alias("segment_min_credit_limit_plus_age_p50"),
    )
)
