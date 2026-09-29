from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Singapore", "Bangkok", "Chiang Mai"]))
    .select(
        "id",
        "region",
        F.length("store_name").alias("store_name_length"),
        F.upper("region").alias("region_upper"),
    )
    .orderBy("region", "region_upper")
)
