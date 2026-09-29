from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2023-10-18")
    .groupBy("region")
    .pivot("city", ["Tokyo", "Singapore", "Bangkok", "Chiang Mai"])
    .agg(F.count("sqft"))
)
