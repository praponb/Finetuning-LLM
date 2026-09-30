from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .pivot("city", ["Chiang Mai", "London", "New York"])
    .agg(F.max("sqft"))
)
