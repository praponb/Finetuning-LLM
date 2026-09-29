from pyspark.sql import functions as F

valid = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-07-16").cast("date"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        (F.col("credit_limit") * F.col("age")).alias("credit_limit_times_age"),
        F.coalesce("segment", F.lit("N/A")).alias("resolved_segment"),
    )
)

result = (
    valid
    .groupBy("country")
    .pivot("segment", ["Corporate", "Consumer"])
    .agg(F.avg("credit_limit"))
)
