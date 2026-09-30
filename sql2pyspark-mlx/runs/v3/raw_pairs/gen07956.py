from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") <= "2023-09-01") & ((F.col("sqft") >= 10119) | (F.year("opened_date") == 2024)))
    .groupBy("city", "region")
    .agg(F.countDistinct("sqft").alias("unique_citys"))
    .orderBy("city")
)
