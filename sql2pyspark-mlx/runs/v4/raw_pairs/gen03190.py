from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age_whole").desc(), "id")
w2 = Window.partitionBy("city", "country").orderBy("credit_limit", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country", "city").orderBy(F.col("last_known_credit_limit").desc())
w5 = Window.partitionBy("city", "country")
w6 = Window.partitionBy("country").orderBy(F.col("credit_limit_avg_5_prior").asc_nulls_last())
w7 = Window.partitionBy("city")
w8 = Window.orderBy(F.col("last_known_credit_limit").desc_nulls_first())
w9 = Window.partitionBy("segment").orderBy("credit_limit_share")
w10 = Window.partitionBy("country", "segment")

base = (
    df_customers
    .filter(F.col("is_active"))
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.ceil("age").alias("age_whole"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("resolved_segment"),
    )
)

metrics = (
    base
    .withColumn("age_whole_bucket", F.ntile(5).over(w))
    .withColumn("credit_limit_avg_5_prior", F.avg("credit_limit").over(w2))
    .withColumn("last_known_credit_limit", F.last("credit_limit", ignorenulls=True).over(w3))
)

matched_keys = (
    metrics
    .select("country")
    .intersectAll(
        df_customers
        .filter(F.col("age") == 38)
        .select("country")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.percent_rank().over(w4).alias("last_known_credit_limit_percent_rank"),
        F.round((F.col("credit_limit_avg_5_prior") / F.sum("credit_limit_avg_5_prior").over(w5)), 4).alias("credit_limit_avg_5_prior_share"),
        F.dense_rank().over(w6).alias("credit_limit_avg_5_prior_dense_rank"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w7)), 4).alias("credit_limit_share"),
        F.dense_rank().over(w8).alias("last_known_credit_limit_dense_rank"),
    )
)

kept_keys = (
    enriched
    .select("segment", "city")
    .intersect(
        df_customers
        .filter(F.col("age") < 64)
        .select("segment", "city")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

metrics_customers = (
    kept_rows
    .select(
        "*",
        F.dense_rank().over(w9).alias("credit_limit_share_dense_rank"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w7)), 4).alias("credit_limit_share_2"),
        F.sum("credit_limit_share").over(w10).alias("country_sum_credit_limit_share"),
        F.min("age_whole").over(w7).alias("city_min_age_whole"),
    )
)

base_customers = (
    metrics_customers
    .filter((F.col("signup_date") >= F.lit("2023-07-30").cast("date")) & F.col("segment").isNotNull() & F.col("last_known_credit_limit_percent_rank").between(0.48, 0.49))
    .withColumn("city_min_age_whole_rounded", F.round("city_min_age_whole", 1))
)

result = (
    base_customers
    .select(
        "id",
        "country",
        "segment",
        "signup_date",
        "credit_limit",
        "age_whole_bucket",
        "last_known_credit_limit",
        "last_known_credit_limit_percent_rank",
        "credit_limit_avg_5_prior_share",
    )
)
