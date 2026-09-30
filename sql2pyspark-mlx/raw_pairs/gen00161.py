from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_customers
    .filter(F.col("name").like("bo%"))
    .withColumn("running_min_age", F.min("age").over(w))
    .withColumn("running_max_age", F.max("age").over(w))
    .withColumn("running_sum_age", F.sum("age").over(w))
)

result = (
    with_windows
    .groupBy("city")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
    )
    .orderBy(F.col("th_events").asc_nulls_first())
)
