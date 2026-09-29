from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNull() & F.col("store_name").like("%Riverside%") & F.col("city").isin(["Chiang Mai", "Bangkok", "London"]))
    .select(
        "id",
        "city",
        "sqft",
        F.round((F.col("sqft") * 0.9), 2).alias("sqft_adjusted"),
    )
)
