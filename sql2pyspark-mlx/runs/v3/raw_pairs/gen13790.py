from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.quarter("opened_date").alias("opened_quarter"),
        F.month("opened_date").alias("opened_month"),
        "city",
    )
    .agg(
        F.sum(F.when(F.col("region") == "North", F.col("sqft")).otherwise(0)).alias("north_sqft"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .filter(F.col("north_sqft") > 38)
    .orderBy(F.col("stddev_sqft").desc())
)
