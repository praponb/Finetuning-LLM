from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy(F.col("start_date").asc_nulls_first(), "id")

latest = (
    df_subscriptions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("monthly_fee") >= 227, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee")).otherwise(0)).alias("expired_monthly_fee"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
    )
)
