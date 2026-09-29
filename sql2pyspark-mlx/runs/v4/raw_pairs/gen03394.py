from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.countDistinct("sqft").alias("unique_citys"))
)
