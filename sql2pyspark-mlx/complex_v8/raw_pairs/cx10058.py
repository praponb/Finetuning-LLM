from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_region_segment_or_unknown_by = Window.partitionBy("region", "segment_or_unknown").orderBy("joined_on", "id")
w_segment_or_unknown_by_joined = Window.partitionBy("segment_or_unknown").orderBy(F.col("joined_on").asc_nulls_last(), "id")
w_segment_or_unknown_by_joined_2 = Window.partitionBy("segment_or_unknown").orderBy(F.col("joined_on").asc_nulls_last())
w_segment_or_unknown_by_joined_3 = Window.partitionBy("segment_or_unknown").orderBy("joined_on")
w_region_segment_or_unknown_by_2 = Window.partitionBy("region", "segment_or_unknown").orderBy("joined_on")
w_segment_or_unknown_by_joined_4 = Window.partitionBy("segment_or_unknown").orderBy("joined_on", "id")
w_joined_on_tercile = Window.partitionBy("joined_on_tercile")
w_next_joined_on_month_next_jo = Window.partitionBy(F.month("next_joined_on"), F.year("next_joined_on_2")).orderBy("joined_on_tercile", "id").rowsBetween(-6, -1)
w_region_by_joined_on_tercile = Window.partitionBy("region").orderBy(F.col("joined_on_tercile").desc(), "id").rowsBetween(-1, Window.currentRow)

prepared = (
    df_policyholders
    .filter((F.col("joined_on") < F.lit("2025-09-02").cast("date")) & (F.length(F.trim(F.col("holder_name"))) >= 5))
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        F.coalesce(F.col("segment"), F.lit("UNKNOWN")).alias("segment_or_unknown"),
    )
)

windowed = (
    prepared
    .select(
        "*",
        F.lag("joined_on").over(w_region_segment_or_unknown_by).alias("prev_joined_on"),
        F.lead("joined_on").over(w_segment_or_unknown_by_joined).alias("next_joined_on"),
        F.dense_rank().over(w_segment_or_unknown_by_joined_2).alias("joined_on_dense_rank"),
        F.ntile(3).over(w_region_segment_or_unknown_by).alias("joined_on_tercile"),
        F.rank().over(w_segment_or_unknown_by_joined_3).alias("joined_on_rank"),
        F.round(F.percent_rank().over(w_region_segment_or_unknown_by_2), 3).alias("joined_on_percent_rank"),
        F.lead("joined_on").over(w_segment_or_unknown_by_joined_4).alias("next_joined_on_2"),
        F.row_number().over(w_segment_or_unknown_by_joined).alias("joined_on_seq"),
    )
)

windowed_2 = (
    windowed
    .select(
        "*",
        F.round(F.col("joined_on_seq") - F.avg("joined_on_seq").over(w_joined_on_tercile), 2).alias("joined_on_seq_vs_joined_on_tercile_avg"),
        F.sum("joined_on_tercile").over(w_next_joined_on_month_next_jo).alias("joined_on_tercile_sum_6_prior"),
        F.max("joined_on_dense_rank").over(w_region_by_joined_on_tercile).alias("joined_on_dense_rank_max_2"),
    )
)

result = (
    windowed_2
    .filter(F.col("joined_on_seq_vs_joined_on_tercile_avg").between(-8.0, 9.0))
    .select(
        "id",
        "region",
        "joined_on",
        "segment_or_unknown",
        "joined_on_dense_rank",
        "joined_on_tercile",
        "joined_on_rank",
        "next_joined_on_2",
        "joined_on_seq_vs_joined_on_tercile_avg",
        "joined_on_tercile_sum_6_prior",
        "joined_on_dense_rank_max_2",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("prev_joined_on")).alias("days_since_prev_joined_on"),
    )
    .orderBy(
        F.col("segment_or_unknown").asc_nulls_last(),
        F.col("joined_on_dense_rank").desc_nulls_first(),
    )
)
