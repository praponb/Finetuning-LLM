from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "US") & ((F.col("rating") >= 4.2) | ~F.col("city").isin(["Chiang Mai", "Tokyo"])))
    .groupBy("city")
    .agg(F.min("rating").alias("min_rating"))
    .orderBy("min_rating")
)
