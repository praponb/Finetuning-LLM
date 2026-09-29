from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "sqft",
        F.round("sqft", 1).alias("sqft_rounded"),
        F.concat("region", F.lit("/"), "store_name").alias("region_store_name"),
    )
)
