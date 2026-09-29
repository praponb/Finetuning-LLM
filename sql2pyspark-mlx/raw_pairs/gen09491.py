from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("distinct_status").desc())

result = (
    df_shipments
    .groupBy("carrier", "order_id")
    .agg(
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
        F.max("cost").alias("max_cost"),
        F.countDistinct("status").alias("distinct_status"),
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"),
    )
    .withColumn("distinct_status_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("order_id").desc())
)
