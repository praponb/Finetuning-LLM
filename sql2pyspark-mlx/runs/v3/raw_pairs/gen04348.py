from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Downt%") & (F.col("opened_date") >= "2023-06-01"))
    .groupBy(
        F.when(F.col("city").isin(["Singapore", "London"]), "priority").otherwise("other").alias("city_group"),
        F.year("opened_date").alias("opened_year"),
    )
    .agg(F.avg("sqft").alias("avg_sqft"))
    .filter(F.col("avg_sqft") > 12300)
)
