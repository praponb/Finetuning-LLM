from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") > "2024-04-04")
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
    .filter(F.col("min_sqft") < 13000)
    .select("opened_month", "min_sqft", "stddev_sqft")
    .orderBy("min_sqft")
)
