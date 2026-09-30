from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@gmail.com"))
    .select(
        "id",
        "credit_limit",
        F.concat_ws("-", "country", "segment").alias("country_segment_key"),
    )
)
