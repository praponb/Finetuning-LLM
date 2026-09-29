from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", "city", F.year("opened_date").alias("opened_year"))
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
)
