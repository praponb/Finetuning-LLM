from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-08-01", "2024-07-06"))
    .groupBy("region")
    .pivot("city", ["Tokyo", "Chiang Mai", "New York", "Singapore"])
    .agg(F.sum("sqft"))
)
