from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter(F.col("segment").isNotNull() & F.col("name").like("Noa%"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.concat_ws("-", "city", "email").alias("city_email_key"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

result = (
    filtered
    .groupBy(
        F.date_trunc("quarter", "signup_date").alias("quarter_start"),
        F.date_trunc("month", "signup_date").alias("activity_month"),
    )
    .agg(
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
        F.bit_and("age").alias("age_bit_and"),
    )
    .orderBy("activity_month")
)
