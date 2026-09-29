from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")

with_windows = (
    df_customers
    .withColumn("running_max_age", F.max("age").over(w_recent))
    .withColumn("running_min_age", F.min("age").over(w_recent))
    .withColumn("running_sum_age", F.sum("age").over(w_recent))
    .withColumn("credit_limit_bucket", F.ntile(3).over(w2))
)

result = (
    with_windows
    .groupBy("segment")
    .agg(
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_rows"),
        F.round(F.sum("credit_limit_bucket") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("credit_limit_bucket_per_chiang_mai"),
        F.count("*").alias("row_count"),
        F.avg("running_max_age").alias("avg_running_max_age"),
    )
    .orderBy(F.col("segment").desc_nulls_first())
)
