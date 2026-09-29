from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(10823, 13652) | (F.col("opened_date") >= "2024-02-22"))
    .groupBy(
        F.when(F.col("region").isin(["Central", "North"]), "group_a").otherwise("other").alias("region_group"),
    )
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.max("sqft").alias("max_sqft"),
    )
    .orderBy("max_sqft", F.col("stddev_sqft").desc())
)
