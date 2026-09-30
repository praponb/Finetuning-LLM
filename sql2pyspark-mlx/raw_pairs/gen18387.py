from pyspark.sql import functions as F

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-05-14").cast("date")) & (~F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        F.regexp_replace("name", " ", "_").alias("name_slug"),
        F.greatest("age", "credit_limit").alias("greatest_age_credit_limit"),
        F.coalesce("segment", F.lit("NONE")).alias("segment_filled"),
    )
)

result = (
    valid
    .groupBy("segment")
    .pivot("city", ["London", "Tokyo"])
    .agg(
        F.max("greatest_age_credit_limit").alias("total"),
        F.count(F.lit(1)).alias("n"),
    )
)
