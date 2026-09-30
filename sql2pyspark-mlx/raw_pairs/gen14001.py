from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("region", "city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_stores
    .filter(F.col("store_name").like("%Old%"))
    .withColumn("running_sum_sqft", F.sum("sqft").over(w_recent))
    .withColumn("running_count_sqft", F.count("sqft").over(w_recent))
    .withColumn("running_min_sqft", F.min("sqft").over(w_recent))
    .withColumn("running_avg_sqft", F.avg("sqft").over(w_recent))
    .withColumn("first_known_sqft", F.first("sqft", ignorenulls=True).over(w_grp))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("running_avg_sqft") >= 15100, "LARGE").otherwise("SMALL").alias("running_avg_sqft_tier"),
    )
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count_if(F.col("region") == "South").alias("south_events"),
        F.percentile_approx("sqft", 0.9).alias("sqft_p90"),
    )
)
