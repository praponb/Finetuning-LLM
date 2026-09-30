from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "sqft",
        "city",
        F.concat("region", F.lit(" - "), "store_name").alias("region_store_name"),
    )
    .orderBy(F.col("sqft").desc())
)
