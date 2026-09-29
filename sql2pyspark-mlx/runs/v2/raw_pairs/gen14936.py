from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["Bangkok", "New York", "Tokyo", "Chiang Mai"])
    .agg(F.count("sqft"))
)
