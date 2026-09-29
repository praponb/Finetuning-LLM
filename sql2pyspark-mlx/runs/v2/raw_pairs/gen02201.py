from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.countDistinct("sqft").alias("unique_citys"))
    .filter(F.col("unique_citys") >= 6)
)
