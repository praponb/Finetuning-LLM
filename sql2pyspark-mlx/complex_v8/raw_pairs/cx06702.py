from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment_joined_on_month_by_j = Window.partitionBy("segment", F.date_trunc("month", "joined_on")).orderBy("joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_segment_joined_on_month_by_j_2 = Window.partitionBy("segment", F.date_trunc("month", "joined_on")).orderBy("joined_on", "id")
w_segment_joined_on_month_by_j_3 = Window.partitionBy("segment", F.date_trunc("month", "joined_on")).orderBy("joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_policyholders
    .filter((F.col("joined_on") < F.lit("2025-02-22").cast("date")) & (F.col("region") == "West"))
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "segment",
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.first("joined_on").over(w_segment_joined_on_month_by_j).alias("first_joined_on"),
        F.ntile(4).over(w_segment_joined_on_month_by_j_2).alias("joined_on_quartile"),
        F.last("joined_on", ignorenulls=True).over(w_segment_joined_on_month_by_j_3).alias("last_known_joined_on"),
    )
)

result = (
    metrics
    .cube("segment", "region", "joined_on_quartile")
    .agg(
        F.grouping("segment").alias("is_total_segment"),
        F.round(F.stddev("joined_on_quartile"), 2).alias("stddev_joined_on_quartile"),
    )
    .select(
        F.coalesce(F.col("segment"), F.lit("ALL")).alias("segment"),
        F.coalesce(F.col("region"), F.lit("ALL")).alias("region"),
        "joined_on_quartile",
        "is_total_segment",
        "stddev_joined_on_quartile",
    )
    .orderBy(
        F.col("segment").asc_nulls_last(),
        F.col("region").asc_nulls_last(),
        F.col("joined_on_quartile").asc_nulls_last(),
    )
)
