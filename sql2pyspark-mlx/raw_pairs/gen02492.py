from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("segment") == "Corporate") & F.col("is_active"))
    .select(
        "id",
        "city",
        "segment",
        F.concat_ws("-", "segment", "city").alias("segment_city_key"),
    )
)
