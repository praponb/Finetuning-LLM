from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2024-01-24").cast("date")) & F.col("city").isNotNull() & (F.col("age") > 46))
    .withColumn("country_lower", F.lower("country"))
)

result = (
    base
    .select("id", "segment")
    .intersectAll(
        df_customers
        .filter(F.col("city") == "Tokyo")
        .select("id", "segment")
    )
)
