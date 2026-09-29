from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull() | (F.col("opened_date") >= "2025-06-01"))
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        F.lower("store_name").alias("store_name_lower"),
        F.round("sqft", 1).alias("sqft_rounded"),
    )
)
