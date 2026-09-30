from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("distinct_cities").desc())

result = (
    df_stores
    .groupBy(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
        "city",
    )
    .agg(
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "South", 1)), F.lit(0)), 2).alias("sqft_per_south"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.countDistinct("city").alias("distinct_cities"),
    )
    .withColumn("distinct_cities_dense_rank", F.dense_rank().over(w))
    .orderBy("city")
)
