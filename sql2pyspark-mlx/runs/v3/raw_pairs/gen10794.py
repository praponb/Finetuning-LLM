from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["New York", "Chiang Mai"]))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "region")
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
    .filter(F.col("stddev_sqft") > 69)
)
