from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-10-06").cast("date"))
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("segment") == "Corporate", 1).otherwise(0).alias("is_corporate"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

result = (
    filtered
    .select("city", "segment")
    .intersect(
        df_customers
        .filter(F.col("name").like("%Hen%"))
        .select("city", "segment")
    )
)
