from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-07-28").cast("date")) & F.col("email").isNotNull())
    .withColumn("segment_filled", F.coalesce("segment", F.lit("Unknown")))
    .withColumn("name_length", F.length("name"))
)

result = (
    base
    .select("country", "id")
    .subtract(
        df_customers
        .filter(F.col("city").isin(["Tokyo", "London"]))
        .select("country", "id")
    )
)
