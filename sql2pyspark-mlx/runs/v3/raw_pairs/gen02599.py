from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Siam%") | F.col("sqft").between(7491, 12064))
    .select(
        "store_name",
        "opened_date",
        F.round((F.col("sqft") * 1.15), 2).alias("sqft_scaled"),
        F.concat_ws(",", "store_name", "city").alias("store_name_city_key"),
    )
)
