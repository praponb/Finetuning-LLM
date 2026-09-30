from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") != 10986) | (F.col("sqft") == 12904))
    .select(
        "id",
        "region",
        "store_name",
        F.concat("store_name", F.lit(", "), "region").alias("store_name_region"),
    )
)
