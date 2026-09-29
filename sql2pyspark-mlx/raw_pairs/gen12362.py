from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("num_channels").desc())

result = (
    df_orders
    .groupBy(F.date_trunc("QUARTER", "order_date").alias("quarter_start"), "status")
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.sum("amount").alias("total_amount"),
        F.count_if(F.col("channel") == "web").alias("web_events"),
    )
    .filter(F.col("web_events") < 1000)
    .withColumn("num_channels_dense_rank", F.dense_rank().over(w))
)
