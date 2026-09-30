from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Tow%") | (F.col("city") == "Singapore"))
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.min("sqft").alias("min_sqft"),
    )
    .orderBy("min_opened_date")
)
