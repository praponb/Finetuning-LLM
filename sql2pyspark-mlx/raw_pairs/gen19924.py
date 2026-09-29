from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("region").orderBy(F.col("sqft_stddev").desc())

deduped = (
    df_stores
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .groupBy("region", F.date_trunc("QUARTER", "opened_date").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.percentile_approx("sqft", 0.9).alias("sqft_p90"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
    )
    .filter(F.col("singapore_count") < 1000)
    .withColumn("sqft_stddev_rank", F.rank().over(w2))
    .orderBy(F.col("sqft_p90").desc_nulls_first())
)
