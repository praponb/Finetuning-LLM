from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["Central", "North", "East"]) | (F.col("opened_date") <= "2024-12-01"))
    .groupBy("city")
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.count("*").alias("total_count"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
    )
    .filter(F.col("total_count") >= 2)
)
