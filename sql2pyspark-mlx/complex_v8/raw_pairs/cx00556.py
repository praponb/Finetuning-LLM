from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_dept_or_unknown = Window.partitionBy("level", "dept_or_unknown")
w_level_len_by_credits_full = Window.partitionBy("level_len").orderBy("credits", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_credits_level_by_level_len = Window.partitionBy("credits", "level").orderBy("level_len", "id")
w_dept_by_level_len_ties = Window.partitionBy("dept").orderBy("level_len")
w_dept_by_level_len_running = Window.partitionBy("dept").orderBy("level_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dept_by_level_len = Window.partitionBy("dept").orderBy("level_len", "id")
w_level_len_by_credits = Window.partitionBy("level_len").orderBy("credits", "id")
w_level_len_decile_level_len = Window.partitionBy("level_len_decile", "level_len")
w_level_by_credits_ties = Window.partitionBy("level").orderBy("credits")
w_level_by_credits_running = Window.partitionBy("level").orderBy("credits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_credits_running = Window.orderBy("credits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_credits = Window.orderBy("credits", "id")

filtered_courses = (
    df_courses
    .filter((F.col("dept") == "Art") & (F.col("level").isin("advanced", "intro")))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.coalesce(F.col("dept"), F.lit("UNKNOWN")).alias("dept_or_unknown"),
        F.length(F.trim(F.col("level"))).alias("level_len"),
    )
)

with_trends = (
    filtered_courses
    .select(
        "*",
        F.round(F.col("level_len") - F.avg("level_len").over(w_level_dept_or_unknown), 2).alias("level_len_vs_level_dept_or_unknown_avg"),
        F.first("level_len").over(w_level_len_by_credits_full).alias("first_level_len"),
        F.max("credits").over(w_level_dept_or_unknown).alias("level_dept_or_unknown_max_credits"),
        F.ntile(10).over(w_credits_level_by_level_len).alias("level_len_decile"),
        F.round(F.col("credits") / F.nullif(F.sum("credits").over(w_level_dept_or_unknown), F.lit(0)), 4).alias("credits_share_of_level_dept_or_unknown"),
        (F.col("level_len") - F.lag("level_len").over(w_credits_level_by_level_len)).alias("level_len_change"),
        F.rank().over(w_dept_by_level_len_ties).alias("level_len_rank"),
        F.last("credits", ignorenulls=True).over(w_dept_by_level_len_running).alias("last_known_credits"),
        F.row_number().over(w_dept_by_level_len).alias("level_len_seq"),
        F.lag("credits", 2).over(w_level_len_by_credits).alias("prev2_credits"),
        F.round(F.percent_rank().over(w_dept_by_level_len_ties), 3).alias("level_len_percent_rank"),
    )
)

with_windows = (
    with_trends
    .select(
        "*",
        F.round(F.col("level_len_seq") - F.avg("level_len_seq").over(w_level_len_decile_level_len), 2).alias("level_len_seq_vs_level_len_decile_level_len_avg"),
        F.rank().over(w_level_by_credits_ties).alias("credits_rank"),
        F.sum("prev2_credits").over(w_level_by_credits_running).alias("running_prev2_credits"),
        F.max("level_len_decile").over(w_all_by_credits_running).alias("max_level_len_decile_so_far"),
        F.dense_rank().over(w_level_by_credits_ties).alias("credits_dense_rank"),
        F.rank().over(w_all_by_credits).alias("credits_rank_2"),
    )
)

rollup_base = (
    with_windows
    .groupBy("level_len_decile", "dept")
    .agg(
        F.sum("level_len_decile").alias("total_level_len_decile"),
        F.count(F.when(F.col("level") == "intro", 1)).alias("n_level_intro"),
        F.count("*").alias("n_rows"),
        F.countDistinct("level").alias("distinct_levels"),
        F.count(F.when(F.col("dept") == "Bio", 1)).alias("n_dept_bio"),
    )
)

result = (
    rollup_base
    .filter(F.col("level_len_decile") < 8)
    .select(
        "level_len_decile",
        "dept",
        "n_level_intro",
        "distinct_levels",
        "n_dept_bio",
        F.when(F.col("dept").isin("Bio", "Math"), "group_a").when(F.col("dept") == "Biz", "group_b").otherwise("other").alias("dept_group"),
    )
    .orderBy("distinct_levels")
)
