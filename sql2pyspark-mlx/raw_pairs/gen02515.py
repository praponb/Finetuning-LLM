from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.avg("sqft").over(w_hist).alias("running_avg_sqft"),
        F.count("sqft").over(w_hist).alias("running_count_sqft"),
        F.sum("sqft").over(w_hist).alias("running_sum_sqft"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("sqft") >= 17293, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
    )
    .agg(
        F.count_if(F.col("region") == "South").alias("south_events"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_rows"),
        F.max(F.when(F.col("region") == "East", F.col("running_sum_sqft"))).alias("east_max_running_sum_sqft"),
    )
    .orderBy("sqft_tier")
)
