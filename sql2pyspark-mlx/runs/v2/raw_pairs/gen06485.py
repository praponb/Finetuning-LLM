from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
    .filter(F.col("total_sqft") < 66000)
    .select("opened_year", "total_sqft", "stddev_sqft")
)
