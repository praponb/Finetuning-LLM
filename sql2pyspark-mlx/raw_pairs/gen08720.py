from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("chiang_mai_avg_sqft").desc())

ranked = (
    df_stores
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .groupBy("city", "region", F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "South", 1)), F.lit(0)), 2).alias("sqft_per_south"),
        F.avg(F.when(F.col("city") == "Chiang Mai", F.col("sqft"))).alias("chiang_mai_avg_sqft"),
    )
    .filter(F.col("sqft_per_south") > 0)
    .withColumn("chiang_mai_avg_sqft_percent_rank", F.percent_rank().over(w2))
    .orderBy(F.col("week_start").asc_nulls_last())
)
