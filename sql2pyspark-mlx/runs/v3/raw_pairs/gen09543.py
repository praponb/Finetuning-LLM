from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "New York") & (F.col("city") == "Berlin"))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("min_sqft") < 12400)
)
