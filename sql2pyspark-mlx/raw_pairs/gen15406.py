from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc())

latest = (
    df_stores
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("sqft") >= 14831, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.avg(F.when(F.col("city") == "Singapore", F.col("sqft"))).alias("singapore_avg_sqft"),
        F.avg(F.when(F.col("region") == "North", F.col("sqft"))).alias("north_avg_sqft"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.bit_and("sqft").alias("sqft_bit_and"),
    )
    .orderBy(F.col("avg_sqft").desc_nulls_last())
)
