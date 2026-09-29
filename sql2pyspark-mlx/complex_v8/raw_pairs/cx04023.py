from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_filled_level_by_level = Window.partitionBy("level_filled", "level").orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_filled_level_by_level_2 = Window.partitionBy("level_filled", "level").orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_level_by_level_filled_moving = Window.partitionBy("level").orderBy(F.col("level_filled").desc(), "id").rowsBetween(-1, Window.currentRow)
w_level_filled_level_by_level_3 = Window.partitionBy("level_filled", "level").orderBy("level", "id")
w_all_by_level_full = Window.orderBy(F.col("level").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_team_level_by_level_filled = Window.partitionBy("team", "level").orderBy(F.col("level_filled").asc_nulls_last(), "id")
w_level_level_filled = Window.partitionBy("level", "level_filled")
w_level_by_level = Window.partitionBy("level").orderBy(F.col("level").desc(), "id")
w_level_by_level_filled = Window.partitionBy("level").orderBy(F.col("level_filled").desc(), "id")
w_all_by_level = Window.orderBy(F.col("level").desc_nulls_first(), "id")
w_level_filled_seq_no = Window.partitionBy("level_filled", "seq_no")
w_all_by_level_level_filled_mi = Window.orderBy(F.col("level_level_filled_min_level_filled").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_rows_in_level_level_f = Window.orderBy("rows_in_level_level_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_rows_in_level_level_f_2 = Window.orderBy("rows_in_level_level_filled", "id")

filtered = (
    df_adjusters
    .filter((F.col("level") <= 3) & (F.upper(F.trim(F.col("adjuster_name"))).like("E%")))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled"),
        F.substring(F.trim(F.col("adjuster_name")), 1, 3).alias("adjuster_name_prefix"),
    )
)

windowed = (
    filtered
    .select(
        "id",
        "adjuster_name",
        "team",
        "level_filled",
        "adjuster_name_prefix",
        F.last("level_filled").over(w_level_filled_level_by_level).alias("last_level_filled"),
        F.count(F.lit(1)).over(w_level_filled_level_by_level_2).alias("running_count"),
        F.sum("level").over(w_level_by_level_filled_moving).alias("level_sum_2"),
        (F.col("level_filled") - F.lag("level_filled").over(w_level_filled_level_by_level_3)).alias("level_filled_change"),
        (F.col("level") - F.lag("level").over(w_level_filled_level_by_level_3)).alias("level_change"),
        F.nth_value("level", 2).over(w_all_by_level_full).alias("second_level"),
        F.lag("level_filled").over(w_team_level_by_level_filled).alias("prev_level_filled"),
        F.min("level_filled").over(w_level_level_filled).alias("level_level_filled_min_level_filled"),
        F.round(F.col("level_filled") / F.nullif(F.sum("level_filled").over(w_level_level_filled), F.lit(0)), 4).alias("level_filled_share_of_level_level_filled"),
        (F.col("level") - F.lag("level").over(w_team_level_by_level_filled)).alias("level_change_2"),
        F.lag("level_filled", 3).over(w_level_by_level).alias("prev3_level_filled"),
        F.row_number().over(w_level_by_level_filled).alias("seq_no"),
        F.ntile(4).over(w_all_by_level).alias("level_quartile"),
        F.count(F.lit(1)).over(w_level_level_filled).alias("rows_in_level_level_filled"),
    )
)

with_trends = (
    windowed
    .select(
        "id",
        "adjuster_name",
        "level_filled",
        "last_level_filled",
        "level_sum_2",
        "level_change",
        "prev_level_filled",
        "level_level_filled_min_level_filled",
        "level_change_2",
        "prev3_level_filled",
        "seq_no",
        "level_quartile",
        F.max("level_sum_2").over(w_level_filled_seq_no).alias("level_filled_seq_no_max_level_sum_2"),
        F.round(F.col("seq_no") / F.nullif(F.sum("seq_no").over(w_level_filled_seq_no), F.lit(0)), 4).alias("seq_no_share_of_level_filled_seq_no"),
        F.round(F.col("running_count") / F.nullif(F.sum("running_count").over(w_level_filled_seq_no), F.lit(0)), 4).alias("running_count_share_of_level_filled_seq_no"),
        F.max("level_quartile").over(w_all_by_level_level_filled_mi).alias("level_quartile_max_3"),
        F.sum("prev_level_filled").over(w_level_filled_seq_no).alias("level_filled_seq_no_sum_prev_level_filled"),
        F.first("second_level", ignorenulls=True).over(w_all_by_rows_in_level_level_f).alias("first_known_second_level"),
        F.round(F.col("level_quartile") / F.nullif(F.sum("level_quartile").over(w_level_filled_seq_no), F.lit(0)), 4).alias("level_quartile_share_of_level_filled_seq_no"),
        F.lead("level_change").over(w_all_by_rows_in_level_level_f_2).alias("next_level_change"),
    )
)

summary = (
    with_trends
    .groupBy("level_change_2")
    .agg(
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
        F.sum(F.when(F.upper(F.trim(F.col("adjuster_name"))).like("E%"), 1).otherwise(0)).alias("n_upper_trim_adjuster_name_lik"),
        F.sum(F.when(F.upper(F.trim(F.col("adjuster_name"))).like("F%"), 1).otherwise(0)).alias("n_upper_trim_adjuster_name_lik_2"),
    )
)

result = (
    summary
    .filter(F.size(F.col("adjuster_name_list")) >= 2)
    .select(
        "level_change_2",
        "adjuster_name_list",
        "n_upper_trim_adjuster_name_lik",
        "n_upper_trim_adjuster_name_lik_2",
        (F.floor(F.col("level_change_2") / 2) * 2).alias("level_change_2_bucket"),
        F.when(F.col("level_change_2") >= 0, "large").when(F.col("level_change_2") >= -1, "medium").otherwise("small").alias("level_change_2_tier"),
    )
    .orderBy(F.col("level_change_2_bucket").asc_nulls_last(), "level_change_2")
    .limit(5)
)
