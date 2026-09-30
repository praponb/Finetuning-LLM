from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")

latest = (
    df_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("YEAR", "end_date").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee"))).alias("premium_total_monthly_fee"),
        F.count_if(F.col("status") == "PAUSED").alias("paused_events"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
)
