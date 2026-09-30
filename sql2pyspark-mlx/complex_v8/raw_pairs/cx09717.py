from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_region_segment_by_joined_on = Window.partitionBy("region", "segment").orderBy(F.col("joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_region_joined_on_month_by_jo = Window.partitionBy("region", F.date_trunc("month", "joined_on")).orderBy("joined_on", "id")
w_region_segment_by_joined_on_2 = Window.partitionBy("region", "segment").orderBy(F.col("joined_on").desc())
w_region_by_joined_on_ties = Window.partitionBy("region").orderBy("joined_on")
w_segment_region_by_joined_on = Window.partitionBy("segment", "region").orderBy(F.col("joined_on").asc_nulls_last())
w_segment_region_by_joined_on_2 = Window.partitionBy("segment", "region").orderBy(F.col("joined_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_region_by_joined_on = Window.partitionBy("region").orderBy("joined_on", "id")
w_region_joined_on_month_by_jo_2 = Window.partitionBy("region", F.date_trunc("month", "joined_on")).orderBy("joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_segment_by_joined_on = Window.partitionBy("segment").orderBy(F.col("joined_on").desc_nulls_first(), "id")

scoped_holders = (
    df_policyholders
    .filter(F.col("holder_name").isNotNull())
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "segment",
        F.lower(F.regexp_replace(F.trim(F.col("region")), " ", "_")).alias("region_slug"),
    )
)

with_windows = (
    scoped_holders
    .select(
        "*",
        F.last("joined_on", ignorenulls=True).over(w_region_segment_by_joined_on).alias("last_known_joined_on"),
        F.datediff(F.col("joined_on"), F.lag("joined_on").over(w_region_joined_on_month_by_jo)).alias("days_since_prev_joined_on"),
        F.rank().over(w_region_segment_by_joined_on_2).alias("joined_on_rank"),
        F.round(F.cume_dist().over(w_region_by_joined_on_ties), 3).alias("joined_on_cume_dist"),
        F.round(F.cume_dist().over(w_segment_region_by_joined_on), 3).alias("joined_on_cume_dist_2"),
        F.last("joined_on").over(w_segment_region_by_joined_on_2).alias("last_joined_on"),
        F.datediff(F.col("joined_on"), F.lag("joined_on").over(w_region_by_joined_on)).alias("days_since_prev_joined_on_2"),
        F.first("joined_on", ignorenulls=True).over(w_region_joined_on_month_by_jo_2).alias("first_known_joined_on"),
        F.round(F.cume_dist().over(w_segment_by_joined_on), 3).alias("joined_on_cume_dist_4"),
        F.lead("joined_on").over(w_segment_by_joined_on).alias("next_joined_on"),
        F.row_number().over(w_region_joined_on_month_by_jo).alias("joined_on_seq"),
        F.dense_rank().over(w_region_segment_by_joined_on_2).alias("joined_on_dense_rank"),
        F.datediff(F.col("joined_on"), F.lag("joined_on").over(w_segment_by_joined_on)).alias("days_since_prev_joined_on_3"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "segment",
        "last_known_joined_on",
        "days_since_prev_joined_on",
        "joined_on_rank",
        "joined_on_cume_dist",
        "joined_on_cume_dist_2",
        "days_since_prev_joined_on_2",
        "next_joined_on",
        "joined_on_seq",
        "joined_on_dense_rank",
        F.datediff(F.col("last_joined_on"), F.col("last_known_joined_on")).alias("days_last_known_joined_on_to_last_joined_on"),
    )
    .orderBy("joined_on_dense_rank", F.col("next_joined_on").asc_nulls_last())
)
