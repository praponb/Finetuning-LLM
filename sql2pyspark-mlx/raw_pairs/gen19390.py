from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("running_sum_sqft_tier").orderBy(F.col("running_sum_sqft_p90").desc())

enriched = (
    df_stores
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        F.min("sqft").over(w).alias("running_min_sqft"),
        F.sum("sqft").over(w).alias("running_sum_sqft"),
        F.avg("sqft").over(w).alias("running_avg_sqft"),
        F.count("sqft").over(w).alias("running_count_sqft"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("running_sum_sqft") >= 86000, "HIGH").otherwise("LOW").alias("running_sum_sqft_tier"),
        F.when(F.col("running_min_sqft") >= 12300, "PREMIUM").otherwise("STANDARD").alias("running_min_sqft_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("running_count_sqft", [0.25, 0.5, 0.75]).alias("running_count_sqft_quartiles"),
        F.percentile_approx("running_sum_sqft", 0.9).alias("running_sum_sqft_p90"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("running_count_sqft", 0.9).alias("running_count_sqft_p90"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .withColumn("running_sum_sqft_p90_rank", F.rank().over(w2))
    .orderBy(F.col("running_sum_sqft_p90").desc_nulls_last())
)
