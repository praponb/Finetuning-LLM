from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["North", "South", "East"]))
    .groupBy("region", F.month("opened_date").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("region") == "North", 1).otherwise(0)).alias("north_count"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
    .filter(F.col("stddev_sqft") >= 66)
)
