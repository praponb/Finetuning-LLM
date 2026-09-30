from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", "region")
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.sum(F.when(F.col("region") == "Central", 1).otherwise(0)).alias("central_count"),
    )
    .filter(F.col("sqft_range") < 42)
)
