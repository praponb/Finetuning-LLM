from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment_by_joined_on = Window.partitionBy("segment").orderBy("joined_on", "id")
w_all_by_joined_on_running = Window.orderBy(F.col("joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_segment_by_joined_on_ties = Window.partitionBy("segment").orderBy("joined_on")
w_segment_by_joined_on_full = Window.partitionBy("segment").orderBy("joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_segment_by_joined_on_running = Window.partitionBy("segment").orderBy("joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_holders = (
    df_policyholders
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "segment",
    )
)

metrics = (
    filtered_holders
    .select(
        "*",
        F.datediff(F.col("joined_on"), F.lag("joined_on").over(w_segment_by_joined_on)).alias("days_since_prev_joined_on"),
        F.last("joined_on", ignorenulls=True).over(w_all_by_joined_on_running).alias("last_known_joined_on"),
        F.rank().over(w_segment_by_joined_on_ties).alias("joined_on_rank"),
        F.nth_value("joined_on", 2).over(w_segment_by_joined_on_full).alias("second_joined_on"),
        F.first("joined_on", ignorenulls=True).over(w_segment_by_joined_on_running).alias("first_known_joined_on"),
    )
)

result = (
    metrics
    .groupingSets(
        [["segment", "joined_on_rank", "region"], ["segment"], ["joined_on_rank"], []],
        "segment", "joined_on_rank", "region",
    )
    .agg(
        F.percentile_approx("joined_on_rank", 0.5).alias("median_joined_on_rank"),
        F.count("*").alias("cnt"),
        F.max("second_joined_on").alias("last_second_joined_on"),
    )
    .select(
        F.coalesce(F.col("segment"), F.lit("ALL")).alias("segment"),
        "joined_on_rank",
        F.coalesce(F.col("region"), F.lit("ALL")).alias("region"),
        "median_joined_on_rank",
        "cnt",
        "last_second_joined_on",
    )
    .orderBy(
        F.col("segment").asc_nulls_last(),
        F.col("joined_on_rank").asc_nulls_last(),
        F.col("region").asc_nulls_last(),
    )
)
