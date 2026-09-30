from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.lower(F.trim("store_name")) == "riverside")
    .groupBy("city", "region")
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.min("opened_date").alias("min_opened_date"),
    )
)
