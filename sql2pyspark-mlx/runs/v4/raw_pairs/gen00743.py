from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("sqft", "id")
w_grp = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running_2 = Window.orderBy("sqft", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("city").orderBy("opened_date", "id")
w5 = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_first())

kept = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") < F.col("s.opened_date")), "left_semi")
)

with_windows = (
    kept
    .select(
        "*",
        F.ntile(5).over(w_running).alias("sqft_bucket"),
        F.sum("sqft").over(w_grp).alias("cumulative_sqft"),
        F.avg("sqft").over(w_running_2).alias("sqft_avg_3_prior"),
    )
)

metrics = (
    with_windows
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("sqft_cume_dist", F.cume_dist().over(w5))
)

daily = (
    metrics
    .groupBy("seq_no")
    .agg(
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_rows"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)

result = (
    daily
    .select("seq_no", "central_rows", "city_list")
    .orderBy(F.col("central_rows").desc())
)
