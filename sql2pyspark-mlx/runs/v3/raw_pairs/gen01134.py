from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", "region")
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
    )
    .filter(F.col("sqft_range") < 26)
    .orderBy(F.col("city").desc())
)
