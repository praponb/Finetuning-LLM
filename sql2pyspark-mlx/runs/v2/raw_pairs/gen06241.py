from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "region",
        "city",
        "opened_date",
        F.concat("city", F.lit("/"), "region").alias("city_region"),
    )
    .orderBy("opened_date", F.col("region").desc())
)
