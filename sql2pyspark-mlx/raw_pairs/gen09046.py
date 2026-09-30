from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .pivot("city", ["Chiang Mai", "Tokyo", "London"])
    .agg(F.sum("sqft"))
)
