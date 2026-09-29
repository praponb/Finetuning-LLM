from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") != 10456)
    .groupBy("region")
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .filter(F.col("total_sqft") > 40000)
    .select("region", "total_sqft", "first_opened_date")
    .orderBy("region")
    .limit(50)
)
