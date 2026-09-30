from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-04-25").cast("date"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.col("age").cast("string").alias("age_num"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

result = (
    filtered
    .select("city")
    .subtract(
        df_customers
        .filter(~F.col("segment").isin(["Corporate", "Consumer"]))
        .select("city")
    )
)
