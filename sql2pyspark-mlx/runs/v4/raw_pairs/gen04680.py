from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Flagship%") & F.col("region").isNull())
    .select(
        "id",
        "region",
        F.col("sqft").cast("string").alias("sqft_str"),
        F.col("sqft").cast("double").alias("sqft_str_2"),
    )
)
