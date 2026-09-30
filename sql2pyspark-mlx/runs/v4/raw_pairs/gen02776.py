from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc())
w_ordered = Window.orderBy(F.col("sqft").desc(), "id")
w_grp = Window.partitionBy("region").orderBy("is_east", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("region")
w6 = Window.partitionBy("region").orderBy("sqft", "id")

valid = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.when(F.col("region") == "East", 1).otherwise(0).alias("is_east"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") <= F.col("s.opened_date")), "left_anti")
)

with_windows = (
    kept
    .withColumn("sqft_rank", F.rank().over(w))
    .withColumn("event_seq", F.row_number().over(w_ordered))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w_grp))
)

with_windows_stores = (
    with_windows
    .select(
        "*",
        F.avg("event_seq").over(w_running).alias("event_seq_avg_5_prior"),
        F.sum("event_seq").over(w5).alias("region_sum_event_seq"),
        F.lag("is_east", 1).over(w6).alias("prev_is_east"),
    )
)

result = (
    with_windows_stores
    .groupBy("region")
    .agg(
        F.max(F.when(F.col("region") == "North", F.col("sqft"))).alias("north_max_sqft"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_rows"),
    )
    .orderBy("north_max_sqft")
)
