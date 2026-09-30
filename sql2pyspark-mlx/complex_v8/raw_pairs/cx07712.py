from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_region = Window.partitionBy("region")
w_all_by_days_since_joined_on = Window.orderBy(F.col("days_since_joined_on").asc_nulls_last())
w_all_by_days_since_joined_on_2 = Window.orderBy(F.col("days_since_joined_on").asc_nulls_last(), "id")
w_segment_region = Window.partitionBy("segment", "region")
w_all_by_days_since_joined_on_3 = Window.orderBy(F.col("days_since_joined_on").desc(), "id")
w_all_by_joined_on_moving_2 = Window.orderBy(F.col("joined_on").desc(), "id").rowsBetween(-2, Window.currentRow)

base = (
    df_policyholders
    .filter(F.col("risk_score") > 0.3)
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "segment",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("joined_on")).alias("days_since_joined_on"),
    )
)

windowed = (
    base
    .select(
        "id",
        "holder_name",
        "region",
        "segment",
        F.size(F.collect_set("segment").over(w_region)).alias("distinct_segment_in_region"),
        F.round(F.percent_rank().over(w_all_by_days_since_joined_on), 3).alias("days_since_joined_on_percent_rank"),
        (F.col("days_since_joined_on") - F.lag("days_since_joined_on").over(w_all_by_days_since_joined_on_2)).alias("days_since_joined_on_change"),
        F.min("days_since_joined_on").over(w_segment_region).alias("segment_region_min_days_since_joined_on"),
        F.lag("joined_on").over(w_all_by_days_since_joined_on_3).alias("prev_joined_on"),
        F.rank().over(w_all_by_days_since_joined_on_2).alias("days_since_joined_on_rank"),
        F.max("days_since_joined_on").over(w_all_by_joined_on_moving_2).alias("days_since_joined_on_max_3"),
    )
)

leaders = (
    windowed
    .filter(F.col("days_since_joined_on_rank") == 1)
    .drop("days_since_joined_on_rank")
)

result = (
    leaders
    .select(
        "id",
        "holder_name",
        "segment",
        "segment_region_min_days_since_joined_on",
        "prev_joined_on",
        "days_since_joined_on_max_3",
        F.date_add(F.col("prev_joined_on"), 365).alias("prev_joined_on_plus_365d"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("prev_joined_on")).alias("days_since_prev_joined_on"),
    )
    .orderBy(F.col("days_since_prev_joined_on").asc())
)
