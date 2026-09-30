from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("sqft_tier").orderBy(F.col("avg_sqft").desc())

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 13090, "HIGH").otherwise("LOW").alias("sqft_tier"),
        F.date_trunc("year", "opened_date").alias("year_start"),
    )
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.avg(F.when(F.col("city") == "Chiang Mai", F.col("sqft"))).alias("chiang_mai_avg_sqft"),
    )
    .withColumn("avg_sqft_percent_rank", F.percent_rank().over(w))
)
