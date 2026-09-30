from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "Berlin")
    .select(
        "age",
        F.concat_ws("/", "country", "segment").alias("country_segment_key"),
        F.length("city").alias("city_length"),
    )
    .orderBy(F.col("city_length").desc(), F.col("age").desc())
)
