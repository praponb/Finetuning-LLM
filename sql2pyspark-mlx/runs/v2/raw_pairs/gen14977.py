from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.sum(F.when(F.col("region") == "North", F.col("sqft")).otherwise(0)).alias("north_sqft"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("min_sqft") >= 15500)
    .drop("min_sqft")
    .orderBy(F.col("opened_quarter").desc())
)
