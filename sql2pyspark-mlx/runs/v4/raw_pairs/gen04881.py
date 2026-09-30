from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-11-20").cast("date")) & F.col("city").isNotNull())
    .select("*", F.round("age", 1).alias("age_rounded"))
)

result = (
    filtered
    .select("city", "country")
    .exceptAll(
        df_customers
        .filter(F.length("email") >= 14)
        .select("city", "country")
    )
)
