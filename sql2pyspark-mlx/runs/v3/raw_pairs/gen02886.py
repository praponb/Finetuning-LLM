from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Bangkok")
    .select(
        "id",
        "opened_date",
        F.floor("sqft").alias("sqft_whole"),
        F.length("city").alias("city_length"),
    )
)
