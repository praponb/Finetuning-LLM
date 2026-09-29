from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-08-26").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.greatest("credit_limit", "age").alias("greatest_credit_limit_age"),
    )
)

result = (
    base
    .groupBy(F.when(F.col("age") >= 73, "LARGE").otherwise("SMALL").alias("age_tier"))
    .agg(
        F.percentile_approx("greatest_credit_limit_age", 0.95).alias("greatest_credit_limit_age_p95"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.percentile_approx("greatest_credit_limit_age", 0.9).alias("greatest_credit_limit_age_p90"),
    )
    .orderBy(F.col("greatest_credit_limit_age_p90").asc_nulls_last())
)
