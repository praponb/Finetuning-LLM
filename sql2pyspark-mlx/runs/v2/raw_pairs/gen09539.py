from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("sqft").alias("unique_citys"),
    )
    .filter(F.col("unique_citys") > 4)
    .orderBy("cnt")
)
