from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_dept = Window.partitionBy("level", "dept")
w_dept_by_credits = Window.partitionBy("dept").orderBy(F.col("credits").asc_nulls_last(), "id")
w_credits_dept = Window.partitionBy("credits", "dept")
w_level_by_credits = Window.partitionBy("level").orderBy(F.col("credits").desc_nulls_first(), "id")
w_dept_by_credits_running = Window.partitionBy("dept").orderBy(F.col("credits").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_credits_level_by_credits_run = Window.partitionBy("credits", "level").orderBy(F.col("credits").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_credits_level_by_credits = Window.partitionBy("credits", "level").orderBy(F.col("credits").asc_nulls_last(), "id")
w_all_by_credits_prior_6 = Window.orderBy("credits", "id").rowsBetween(-6, -1)
w_credits_level_by_credits_tie = Window.partitionBy("credits", "level").orderBy(F.col("credits").asc_nulls_last())
w_credits_by_credits_running = Window.partitionBy("credits").orderBy(F.col("credits").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_credits = Window.orderBy("credits", "id")
w_credits_change = Window.partitionBy("credits_change")
w_next_credits_by_next_credits = Window.partitionBy("next_credits").orderBy(F.col("next_credits").asc_nulls_last())
w_distinct_dept_in_credits_cha = Window.partitionBy("distinct_dept_in_credits_change", "distinct_level_in_credits_dept").orderBy(F.col("total_level_dept_sum_credits").desc_nulls_first(), "distinct_dept_in_credits_change", "distinct_level_in_credits_dept")
w_distinct_dept_in_credits_cha_2 = Window.partitionBy("distinct_dept_in_credits_change", "distinct_level_in_credits_dept").orderBy(F.col("total_level_dept_sum_credits").desc_nulls_first(), "distinct_dept_in_credits_change", "distinct_level_in_credits_dept").rowsBetween(-1, Window.currentRow)
w_distinct_dept_in_credits_cha_3 = Window.partitionBy("distinct_dept_in_credits_change", "distinct_level_in_credits_dept").orderBy(F.col("total_level_dept_sum_credits").desc_nulls_first(), "distinct_dept_in_credits_change", "distinct_level_in_credits_dept").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = df_courses.filter((F.lower(F.col("course_code")).like("%05%")) & (F.col("credits") > 2))

ranked = (
    recent
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        F.count(F.lit(1)).over(w_level_dept).alias("rows_in_level_dept"),
        F.ntile(10).over(w_dept_by_credits).alias("credits_decile"),
        F.size(F.collect_set("level").over(w_credits_dept)).alias("distinct_level_in_credits_dept"),
        F.lead("credits").over(w_level_by_credits).alias("next_credits"),
        F.count(F.lit(1)).over(w_dept_by_credits_running).alias("running_count"),
        F.min("credits").over(w_credits_level_by_credits_run).alias("min_credits_so_far"),
        F.rank().over(w_credits_level_by_credits).alias("credits_rank"),
        F.round(F.avg("credits").over(w_all_by_credits_prior_6), 2).alias("credits_avg_6_prior"),
        F.rank().over(w_credits_level_by_credits_tie).alias("credits_rank_2"),
        F.sum("credits").over(w_level_dept).alias("level_dept_sum_credits"),
        F.row_number().over(w_level_by_credits).alias("credits_seq"),
        F.lead("credits").over(w_dept_by_credits).alias("next_credits_2"),
        F.last("credits", ignorenulls=True).over(w_credits_by_credits_running).alias("last_known_credits"),
        F.count(F.lit(1)).over(w_credits_dept).alias("rows_in_credits_dept"),
        (F.col("credits") - F.lag("credits").over(w_all_by_credits)).alias("credits_change"),
        F.round(F.col("credits") / F.nullif(F.sum("credits").over(w_level_dept), F.lit(0)), 4).alias("credits_share_of_level_dept"),
    )
)

windowed = (
    ranked
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "rows_in_level_dept",
        "distinct_level_in_credits_dept",
        "running_count",
        "credits_rank",
        "credits_avg_6_prior",
        "credits_rank_2",
        "level_dept_sum_credits",
        "credits_seq",
        "next_credits_2",
        "credits_change",
        "credits_share_of_level_dept",
        F.round(F.col("next_credits_2") / F.nullif(F.sum("next_credits_2").over(w_credits_change), F.lit(0)), 4).alias("next_credits_2_share_of_credits_change"),
        F.size(F.collect_set("dept").over(w_credits_change)).alias("distinct_dept_in_credits_change"),
        F.rank().over(w_next_credits_by_next_credits).alias("next_credits_rank"),
        F.round(F.avg("credits_rank_2").over(w_credits_change), 2).alias("credits_change_avg_credits_rank_2"),
    )
)

per_group = (
    windowed
    .groupBy("distinct_dept_in_credits_change", "distinct_level_in_credits_dept")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("dept").alias("distinct_depts"),
        F.sum("level_dept_sum_credits").alias("total_level_dept_sum_credits"),
    )
    .filter(F.col("cnt") >= 2)
)

with_windows = (
    per_group
    .select(
        "distinct_dept_in_credits_change",
        "distinct_level_in_credits_dept",
        "cnt",
        "distinct_depts",
        "total_level_dept_sum_credits",
        F.dense_rank().over(w_distinct_dept_in_credits_cha).alias("total_level_dept_sum_credits_dense_rank"),
        F.round(F.avg("distinct_depts").over(w_distinct_dept_in_credits_cha_2), 2).alias("distinct_depts_avg_2"),
        F.rank().over(w_distinct_dept_in_credits_cha).alias("total_level_dept_sum_credits_rank"),
        F.first("cnt").over(w_distinct_dept_in_credits_cha_3).alias("first_cnt"),
    )
)

result = (
    with_windows
    .select(
        "distinct_dept_in_credits_change",
        "distinct_level_in_credits_dept",
        "cnt",
        "distinct_depts",
        "total_level_dept_sum_credits",
        "total_level_dept_sum_credits_dense_rank",
        "distinct_depts_avg_2",
        "total_level_dept_sum_credits_rank",
        "first_cnt",
        F.round(F.col("cnt") / F.nullif(F.col("first_cnt"), F.lit(0)), 3).alias("cnt_per_first_cnt"),
    )
    .orderBy(
        F.col("distinct_level_in_credits_dept").desc(),
        F.col("first_cnt").desc(),
    )
)
