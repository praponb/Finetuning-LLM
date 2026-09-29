from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("carrier").orderBy("cost", "id")
w3 = Window.partitionBy("carrier").orderBy("delivered_date", "id")

scored = (
    df_shipments
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w))
    .withColumn("cost_bucket", F.ntile(10).over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)

result = (
    scored
    .groupBy("order_id")
    .agg(
        F.max(F.when(F.col("status") == "LOST", F.col("first_known_cost"))).alias("lost_max_first_known_cost"),
        F.count("*").alias("event_count"),
        F.count("*").alias("row_count"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.round(F.avg("cost_bucket"), 2).alias("avg_cost_bucket"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cost_stddev").desc_nulls_first())
)
