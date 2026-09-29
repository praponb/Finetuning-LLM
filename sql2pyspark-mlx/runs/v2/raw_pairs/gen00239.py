from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull() & F.month("opened_date").isin([6, 7, 10]))
    .select(
        "id",
        "opened_date",
        F.upper("city").alias("city_upper"),
        F.concat("region", F.lit("/"), "city").alias("region_city"),
    )
)
