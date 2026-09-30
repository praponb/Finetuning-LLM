from pyspark.sql import functions as F

recent_stores = df_stores.filter(F.col("opened_date").between("2023-07-01", "2024-08-01"))

agg = (
    recent_stores
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.countDistinct("sqft").alias("unique_citys"))
)

result = agg
