from pyspark.sql import functions as F

result = (
    df_stores
    .filter((~F.col("city").isin(["London", "Bangkok", "Tokyo"]) & (F.col("city") != "Bangkok")) | (F.col("city") == "Bangkok"))
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("region") == "South", 1).otherwise(0)).alias("south_count"),
        F.count("sqft").alias("sqft_count"),
    )
)
