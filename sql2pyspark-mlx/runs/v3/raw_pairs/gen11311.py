from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2024-11-23", "2024-12-01") & (F.col("sqft") != 9345))
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.sum("sqft").alias("total_sqft"),
        F.countDistinct("sqft").alias("unique_citys"),
    )
    .orderBy("total_sqft")
)
