from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")
w_part = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("sale_date", "id")
w_running = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_first())
w5 = Window.partitionBy("product_id").orderBy("seq_no", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy("prev_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy(F.col("units_percent_rank").desc(), "id")
w8 = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").asc_nulls_first())
w9 = Window.partitionBy("store_id").orderBy(F.col("prev_revenue").desc(), "id")
w10 = Window.partitionBy("product_id").orderBy("rank_in_group", "id")
w11 = Window.partitionBy("store_id", "product_id")
w12 = Window.partitionBy("product_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy("last_known_revenue", "id")
w14 = Window.partitionBy("product_id", "store_id")

scored = (
    df_store_sales
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("units_bucket", F.ntile(4).over(w_part))
    .withColumn("prev_revenue", F.lag("revenue", 1).over(w3))
    .withColumn("units_percent_rank", F.percent_rank().over(w_running))
)

metrics = (
    scored
    .withColumn("units_percent_rank_avg_5", F.avg("units_percent_rank").over(w5))
    .withColumn("last_known_revenue", F.last("revenue", ignorenulls=True).over(w6))
    .withColumn("units_percent_rank_bucket", F.ntile(4).over(w7))
)

latest = (
    metrics
    .withColumn("rank_in_group", F.rank().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    latest
    .select(
        "*",
        F.ntile(10).over(w9).alias("prev_revenue_bucket"),
        F.lag("units_bucket", 1).over(w10).alias("prev_units_bucket"),
        F.count("units_percent_rank_bucket").over(w11).alias("store_id_count_units_percent_rank_bucket"),
        F.sum("prev_revenue").over(w12).alias("running_prev_revenue"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.lag("units_percent_rank_avg_5", 1).over(w13).alias("prev_units_percent_rank_avg_5"),
        F.round((F.col("store_id_count_units_percent_rank_bucket") / F.sum("store_id_count_units_percent_rank_bucket").over(w14)), 4).alias("store_id_count_units_percent_rank_bucket_share"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("activity_month"))
    .agg(
        F.sum("units").alias("total_units"),
        F.round(F.avg("store_id_count_units_percent_rank_bucket"), 2).alias("avg_store_id_count_units_percent_rank_bucket"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").asc_nulls_first())
)
