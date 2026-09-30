from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("region").orderBy(F.col("latest_rank").desc(), "id")
w3 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w4 = Window.partitionBy("region").orderBy("dedup_rank", "id")

valid = (
    df_stores
    .filter(F.col("city") == "Singapore")
    .withColumn("clean_store_name", F.trim("store_name"))
    .withColumn("region_clean", F.coalesce("region", F.lit("OTHER")))
)

kept = (
    valid
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") >= F.col("s.opened_date")), "left_semi")
)

deduped = (
    kept
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    latest
    .filter(F.col("region") == "South")
    .select(
        "id",
        "region",
        "latest_rank",
        F.ntile(3).over(w3).alias("sqft_bucket"),
        F.lag("sqft", 2).over(w4).alias("prev_sqft"),
    )
)

result = (
    with_windows
    .groupBy("region")
    .agg(
        F.size(F.collect_set("region")).alias("num_regions"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_rows"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.avg("prev_sqft").alias("avg_prev_sqft"),
        F.count_if(F.col("region") == "East").alias("east_events"),
    )
    .orderBy("num_regions")
)
