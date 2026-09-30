from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_joined_on_month_holde = Window.partitionBy(F.month("holder_joined_on"), "holder_id").orderBy(F.col("holder_joined_on").desc_nulls_first(), "id")
w_all_by_holder_joined_on = Window.orderBy("holder_joined_on", "id")
w_holder_segment_by_holder_joi = Window.partitionBy("holder_segment").orderBy("holder_joined_on")
w_holder_segment_by_holder_joi_2 = Window.partitionBy("holder_segment").orderBy("holder_joined_on", "id")
w_all_by_holder_joined_on_full = Window.orderBy("holder_joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_joined_on_month_holde_2 = Window.partitionBy(F.month("holder_joined_on"), "holder_id").orderBy(F.col("holder_joined_on").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_joined_on_rank_2 = Window.partitionBy("holder_joined_on_rank_2")
w_next3_holder_joined_on_month = Window.partitionBy(F.month("next3_holder_joined_on")).orderBy(F.col("prev2_holder_joined_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_holder_joined_on_perc = Window.orderBy("holder_joined_on_percent_rank_2", "id")
w_next3_holder_joined_on_month_2 = Window.partitionBy(F.month("next3_holder_joined_on")).orderBy(F.col("prev2_holder_joined_on").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_policies
    .filter((F.col("status").isin("EXPIRED", "CANCELLED", "ACTIVE")) & (F.col("premium") >= 2289.0))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
    )
)

joined = (
    valid.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

metrics = (
    joined
    .select(
        "id",
        "holder_id",
        "holder_segment",
        "holder_joined_on",
        F.lead("holder_joined_on", 3).over(w_holder_joined_on_month_holde).alias("next3_holder_joined_on"),
        F.round(F.percent_rank().over(w_all_by_holder_joined_on), 3).alias("holder_joined_on_percent_rank"),
        F.lead("holder_joined_on").over(w_all_by_holder_joined_on).alias("next_holder_joined_on"),
        F.round(F.percent_rank().over(w_holder_segment_by_holder_joi), 3).alias("holder_joined_on_percent_rank_2"),
        F.lag("holder_joined_on", 2).over(w_holder_segment_by_holder_joi_2).alias("prev2_holder_joined_on"),
        F.nth_value("holder_joined_on", 2).over(w_all_by_holder_joined_on_full).alias("second_holder_joined_on"),
        F.rank().over(w_holder_segment_by_holder_joi).alias("holder_joined_on_rank"),
        F.lag("holder_joined_on").over(w_all_by_holder_joined_on).alias("prev_holder_joined_on"),
        F.first("holder_joined_on").over(w_all_by_holder_joined_on_full).alias("first_holder_joined_on"),
        F.ntile(10).over(w_holder_segment_by_holder_joi_2).alias("holder_joined_on_decile"),
        F.last("holder_joined_on", ignorenulls=True).over(w_holder_joined_on_month_holde_2).alias("last_known_holder_joined_on"),
        F.rank().over(w_holder_segment_by_holder_joi_2).alias("holder_joined_on_rank_2"),
    )
)

annotated = (
    metrics
    .select(
        "*",
        (F.floor(F.col("holder_joined_on_rank") / 2) * 2).alias("holder_joined_on_rank_bucket"),
        F.least(F.col("holder_joined_on_decile"), F.col("holder_joined_on_rank")).alias("least_holder_joined_on_decile_holder_joined_on_r"),
    )
)

with_trends = (
    annotated
    .select(
        "id",
        "holder_id",
        "holder_segment",
        "holder_joined_on",
        "next3_holder_joined_on",
        "holder_joined_on_percent_rank",
        "prev2_holder_joined_on",
        "second_holder_joined_on",
        "holder_joined_on_rank",
        "prev_holder_joined_on",
        "first_holder_joined_on",
        "holder_joined_on_decile",
        "holder_joined_on_rank_2",
        "holder_joined_on_rank_bucket",
        "least_holder_joined_on_decile_holder_joined_on_r",
        F.size(F.collect_set("holder_id").over(w_holder_joined_on_rank_2)).alias("distinct_holder_id_in_holder_joined_on_rank_2"),
        F.min("holder_joined_on_rank_2").over(w_next3_holder_joined_on_month).alias("min_holder_joined_on_rank_2_so_far"),
        F.first("holder_joined_on_rank", ignorenulls=True).over(w_next3_holder_joined_on_month).alias("first_known_holder_joined_on_rank"),
        F.row_number().over(w_all_by_holder_joined_on_perc).alias("holder_joined_on_percent_rank_2_seq"),
        F.count(F.lit(1)).over(w_holder_joined_on_rank_2).alias("rows_in_holder_joined_on_rank_2"),
        F.round(F.avg("holder_joined_on_rank").over(w_next3_holder_joined_on_month_2), 2).alias("holder_joined_on_rank_avg_3"),
    )
)

result = (
    with_trends
    .filter(F.col("prev_holder_joined_on").between(F.lit("2024-02-22").cast("date"), F.lit("2025-05-01").cast("date")))
    .select(
        "id",
        "holder_id",
        "holder_segment",
        "holder_joined_on",
        "next3_holder_joined_on",
        "holder_joined_on_percent_rank",
        "prev2_holder_joined_on",
        "second_holder_joined_on",
        "holder_joined_on_rank",
        "prev_holder_joined_on",
        "first_holder_joined_on",
        "holder_joined_on_decile",
        "holder_joined_on_rank_2",
        "holder_joined_on_rank_bucket",
        "least_holder_joined_on_decile_holder_joined_on_r",
        "min_holder_joined_on_rank_2_so_far",
        "first_known_holder_joined_on_rank",
        "holder_joined_on_percent_rank_2_seq",
        "holder_joined_on_rank_avg_3",
        (F.col("holder_joined_on_decile") + F.col("holder_joined_on_rank_2")).alias("holder_joined_on_decile_plus_holder_joined_on_ra"),
    )
)
