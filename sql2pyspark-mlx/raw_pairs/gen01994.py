from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(F.countDistinct("sqft").alias("unique_citys"))
    .filter(F.col("unique_citys") > 7)
    .select("city", "unique_citys")
)
