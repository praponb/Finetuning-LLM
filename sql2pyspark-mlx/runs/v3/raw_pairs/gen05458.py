from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "opened_date",
        "sqft",
        F.concat("region", F.lit("/"), "city").alias("region_city"),
    )
    .orderBy(F.col("sqft").desc())
)
