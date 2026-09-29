from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy(F.year("signup_date")).orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("segment").orderBy("credit_limit", "id")
w3 = Window.orderBy(F.col("credit_limit_bucket_2").asc_nulls_first())
w4 = Window.partitionBy(F.year("signup_date")).orderBy(F.col("age").desc())
w5 = Window.orderBy(F.col("credit_limit_bucket_2").asc_nulls_last())

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-04-18").cast("date")) & ~F.col("is_active"))
    .select(
        "*",
        F.substring("email", 1, 2).alias("email_prefix"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_clean"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("country") != "US")
    .select(
        "id",
        "email",
        "signup_date",
        "age",
        "email_prefix",
        "segment_clean",
        F.ntile(10).over(w_grp).alias("credit_limit_bucket"),
        F.ntile(5).over(w2).alias("credit_limit_bucket_2"),
    )
)

kept_keys = (
    with_windows
    .select("id")
    .subtract(
        df_customers
        .filter(~F.col("is_active"))
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows_customers = (
    verified
    .select(
        "email",
        "signup_date",
        "age",
        "segment_clean",
        "credit_limit_bucket",
        F.percent_rank().over(w3).alias("credit_limit_bucket_2_percent_rank"),
        F.dense_rank().over(w4).alias("age_dense_rank"),
        F.cume_dist().over(w5).alias("credit_limit_bucket_2_cume_dist"),
    )
)

filtered = (
    with_windows_customers
    .filter(F.col("signup_date") >= F.lit("2023-05-18").cast("date"))
    .select(
        "email",
        "signup_date",
        "age",
        "segment_clean",
        "credit_limit_bucket",
        "credit_limit_bucket_2_percent_rank",
        "age_dense_rank",
        "credit_limit_bucket_2_cume_dist",
        (F.col("age_dense_rank") + F.col("age")).alias("age_dense_rank_plus_age"),
    )
)

cleaned_customers = (
    filtered
    .filter(F.col("signup_date") > F.lit("2023-07-31").cast("date"))
    .select(
        "*",
        F.greatest("age_dense_rank", "age").alias("greatest_age_dense_rank_age"),
    )
)

result = (
    cleaned_customers
    .groupBy(
        F.when(F.col("age") >= 58, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
        F.when(F.col("credit_limit_bucket") >= 9, "LARGE").otherwise("SMALL").alias("credit_limit_bucket_tier"),
    )
    .agg(
        F.min("age").alias("min_age"),
        F.round(F.avg("credit_limit_bucket_2_percent_rank"), 2).alias("avg_credit_limit_bucket_2_percent_rank"),
        F.percentile_approx("credit_limit_bucket", [0.25, 0.5, 0.75]).alias("credit_limit_bucket_quartiles"),
        F.avg("age").alias("avg_age"),
        F.percentile_approx("age_dense_rank", [0.25, 0.5, 0.75]).alias("age_dense_rank_quartiles"),
    )
)
