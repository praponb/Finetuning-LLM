from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("discount_tier").orderBy(F.col("avg_discount").desc())

result = (
    df_orders
    .groupBy(
        F.when(F.col("discount") >= 0.35, "HIGH").otherwise("LOW").alias("discount_tier"),
        "channel",
        F.date_trunc("WEEK", "order_date").alias("week_start"),
    )
    .agg(
        F.count_if(F.col("status") == "RETURNED").alias("returned_events"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.corr("amount", "discount").alias("corr_amount_discount"),
    )
    .withColumn("avg_discount_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("returned_events").desc_nulls_first())
)
