from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .pivot("city", ["Bangkok", "Chiang Mai"])
    .agg(F.count("sqft"))
)
