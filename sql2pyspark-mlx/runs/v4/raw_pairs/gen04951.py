from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id")
w2 = Window.orderBy(F.col("revenue").desc_nulls_last())

enriched = (
    df_store_sales
    .withColumn("corr_revenue_units", F.corr("revenue", "units").over(w_recent))
    .withColumn("revenue_rank", F.rank().over(w2))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("revenue") >= 4400, "PREMIUM").otherwise("STANDARD").alias("revenue_tier"),
        F.date_trunc("MONTH", "sale_date").alias("activity_month"),
    )
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.count("*").alias("event_count"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.sum("revenue").alias("total_revenue"),
    )
    .filter(F.col("event_count") < 1)
    .orderBy(F.col("activity_month").desc_nulls_first())
)
