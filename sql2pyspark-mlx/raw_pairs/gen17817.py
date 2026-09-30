from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy(F.col("is_bangkok").desc(), "id")
w_recent = Window.partitionBy("city", "country")
w3 = Window.partitionBy("country", "segment")
w4 = Window.partitionBy("city")
w5 = Window.partitionBy(F.year("signup_date"), "segment")
w6 = Window.partitionBy("segment", "city").orderBy(F.col("is_bangkok_bucket").desc_nulls_first())
w7 = Window.partitionBy(F.year("signup_date"), "city").orderBy(F.col("age").desc())
w8 = Window.partitionBy("city").orderBy(F.col("city_avg_is_bangkok_bucket").desc())

valid = (
    df_customers
    .filter(F.col("city").isNotNull() & (F.col("age") == 64))
    .withColumn("segment_lower", F.lower("segment"))
    .withColumn("is_bangkok", F.when(F.col("city") == "Bangkok", 1).otherwise(0))
)

with_windows = (
    valid
    .withColumn("is_bangkok_bucket", F.ntile(3).over(w))
    .withColumn("city_max_is_bangkok", F.max("is_bangkok").over(w_recent))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w3)), 4))
)

remaining_keys = (
    with_windows
    .select("segment", "country")
    .intersectAll(
        df_customers
        .filter(F.col("name").like("P%"))
        .select("segment", "country")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

with_windows_customers = (
    reconciled
    .select(
        "id",
        "city",
        "signup_date",
        "is_active",
        "age",
        "segment_lower",
        "is_bangkok",
        "is_bangkok_bucket",
        "credit_limit_share",
        F.round((F.col("is_bangkok_bucket") / F.sum("is_bangkok_bucket").over(w4)), 4).alias("is_bangkok_bucket_share"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w5)), 4).alias("credit_limit_share_2"),
        F.stddev_samp("is_bangkok_bucket").over(w_recent).alias("city_stddev_samp_is_bangkok_bucket"),
        F.dense_rank().over(w6).alias("is_bangkok_bucket_dense_rank"),
        F.sum("age").over(w5).alias("signup_date_year_sum_age"),
    )
)

scored = (
    with_windows_customers
    .filter(F.col("city_stddev_samp_is_bangkok_bucket") != 5)
    .select(
        "city",
        "signup_date",
        "is_active",
        "is_bangkok_bucket",
        "is_bangkok_bucket_share",
        F.rank().over(w7).alias("age_rank"),
        F.avg("is_bangkok_bucket").over(w4).alias("city_avg_is_bangkok_bucket"),
    )
)

base = (
    scored
    .filter((F.col("signup_date") >= F.lit("2023-02-27").cast("date")) & F.col("city").isNotNull())
    .select(
        "city",
        "signup_date",
        "is_active",
        "is_bangkok_bucket",
        "is_bangkok_bucket_share",
        "age_rank",
        "city_avg_is_bangkok_bucket",
        F.substring("city", 1, 4).alias("city_prefix"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

enriched = (
    base
    .select(
        "city",
        "signup_date",
        "city_prefix",
        F.percent_rank().over(w8).alias("city_avg_is_bangkok_bucket_percent_rank"),
        F.sum("age_rank").over(w4).alias("city_sum_age_rank"),
    )
)

result = (
    enriched
    .select(
        "city",
        "signup_date",
        "city_prefix",
        "city_avg_is_bangkok_bucket_percent_rank",
        "city_sum_age_rank",
    )
    .orderBy("signup_date", "city")
)
