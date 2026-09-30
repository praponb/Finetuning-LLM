from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("city", "region").orderBy(F.col("sqft").asc_nulls_first())
w_ordered = Window.partitionBy("region").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("sqft").asc_nulls_last())
w5 = Window.partitionBy("city", "region").orderBy("sqft_dense_rank", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("opened_date", "id")
w7 = Window.orderBy("running_sum_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_stores
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped_stores = (
    deduped
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    deduped_stores
    .select(
        "*",
        F.max("sqft").over(w_ordered).alias("running_max_sqft"),
        F.avg("sqft").over(w_ordered).alias("running_avg_sqft"),
        F.sum("sqft").over(w_ordered).alias("running_sum_sqft"),
        F.min("sqft").over(w_ordered).alias("running_min_sqft"),
        F.dense_rank().over(w4).alias("sqft_dense_rank"),
    )
)

with_windows = (
    enriched
    .filter(F.col("running_avg_sqft") <= 6900)
    .withColumn("last4_count_running_min_sqft", F.count("running_min_sqft").over(w5))
    .withColumn("last4_avg_running_min_sqft", F.avg("running_min_sqft").over(w5))
    .withColumn("last4_min_running_min_sqft", F.min("running_min_sqft").over(w5))
)

result = (
    with_windows
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        "latest_rank",
        "running_avg_sqft",
        "running_min_sqft",
        "last4_count_running_min_sqft",
        F.lead("sqft_dense_rank", 1).over(w6).alias("next_sqft_dense_rank"),
        F.last("running_max_sqft", ignorenulls=True).over(w7).alias("last_known_running_max_sqft"),
    )
)
