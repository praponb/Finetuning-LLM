from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("region").isin(["West", "South"]))
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.max("sqft").alias("max_sqft"),
    )
    .filter(F.col("avg_sqft") > 8400)
    .orderBy("avg_sqft")
)
