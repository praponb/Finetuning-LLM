from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level = Window.partitionBy("level")
w_dept_level = Window.partitionBy("dept", "level")
w_dept_level_by_credits_ties = Window.partitionBy("dept", "level").orderBy("credits")
w_level_dept_by_credits = Window.partitionBy("level", "dept").orderBy("credits", "id")
w_level_dept_by_credits_prior = Window.partitionBy("level", "dept").orderBy("credits", "id").rowsBetween(-1, -1)
w_all_by_credits_moving_6 = Window.orderBy("credits", "id").rowsBetween(-6, Window.currentRow)
w_credits_dept_by_credits_max = Window.partitionBy("credits", "dept").orderBy(F.col("credits_max_7").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_credits = Window.partitionBy("credits")
w_credits_seq_by_rows_in_dept = Window.partitionBy("credits_seq").orderBy("rows_in_dept_level", "id")

filtered_courses = (
    df_courses
    .filter((F.col("level") == "intro") & (F.col("dept") != "Math"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
    )
)

with_windows = (
    filtered_courses
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.max("credits").over(w_level).alias("level_max_credits"),
        F.min("credits").over(w_dept_level).alias("dept_level_min_credits"),
        F.rank().over(w_dept_level_by_credits_ties).alias("credits_rank"),
        F.count(F.lit(1)).over(w_dept_level).alias("rows_in_dept_level"),
        F.row_number().over(w_level_dept_by_credits).alias("credits_seq"),
        F.sum("credits").over(w_level_dept_by_credits_prior).alias("credits_sum_1_prior"),
        F.max("credits").over(w_all_by_credits_moving_6).alias("credits_max_7"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "course_code",
        "credits",
        "level_max_credits",
        "dept_level_min_credits",
        "credits_rank",
        "rows_in_dept_level",
        "credits_seq",
        "credits_sum_1_prior",
        "credits_max_7",
        F.first("dept_level_min_credits").over(w_credits_dept_by_credits_max).alias("first_dept_level_min_credits"),
        F.size(F.collect_set("dept").over(w_credits)).alias("distinct_dept_in_credits"),
        (F.col("credits") - F.lag("credits").over(w_credits_seq_by_rows_in_dept)).alias("credits_change"),
        F.size(F.collect_set("level").over(w_credits)).alias("distinct_level_in_credits"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "credits",
        "level_max_credits",
        "dept_level_min_credits",
        "credits_rank",
        "rows_in_dept_level",
        "credits_seq",
        "credits_sum_1_prior",
        "first_dept_level_min_credits",
        "distinct_dept_in_credits",
        "distinct_level_in_credits",
        F.round(F.col("rows_in_dept_level") / F.nullif(F.col("level_max_credits"), F.lit(0)), 3).alias("rows_in_dept_level_per_level_max_credits"),
    )
    .orderBy("id")
)
