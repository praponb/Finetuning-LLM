from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").asc_nulls_last(), "id")

ranked = (
    df_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy(
        F.date_trunc("YEAR", "end_date").alias("year_start"),
        F.when(F.col("monthly_fee") >= 214, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "PAUSED", F.col("monthly_fee")).otherwise(0)).alias("paused_monthly_fee"),
        F.count_if(F.col("status") == "PAUSED").alias("paused_events"),
        F.percentile_approx("monthly_fee", 0.95).alias("monthly_fee_p95"),
    )
    .filter(F.col("paused_monthly_fee") < 1000000)
    .orderBy(F.col("monthly_fee_tier").desc_nulls_first())
)
