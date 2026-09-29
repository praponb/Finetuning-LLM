from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "opened_date",
        "sqft",
        F.concat("city", F.lit("/"), "region").alias("city_region"),
    )
    .orderBy(F.col("sqft").desc())
)
