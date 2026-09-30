from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_stores
    .withColumn("running_avg_sqft", F.avg("sqft").over(w))
    .withColumn("running_count_sqft", F.count("sqft").over(w))
    .withColumn("running_min_sqft", F.min("sqft").over(w))
)

result = (
    metrics
    .select("id", "opened_date", "sqft", "running_avg_sqft", "running_count_sqft")
    .orderBy(F.col("running_count_sqft").desc_nulls_first())
)
