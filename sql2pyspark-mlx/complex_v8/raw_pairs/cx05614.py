from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_total_credits = Window.orderBy(F.col("total_credits").desc(), "dept")
w_n_upper_trim_course_code_lik = Window.partitionBy("n_upper_trim_course_code_like", "dept")
w_all_by_total_credits_ties = Window.orderBy(F.col("total_credits").desc())
w_total_credits_rank = Window.partitionBy("total_credits_rank")
w_all_by_n_length_trim_course = Window.orderBy(F.col("n_length_trim_course_code_7").desc(), "dept").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_prev_n_length_trim_co = Window.orderBy(F.col("prev_n_length_trim_course_code_7").desc(), "dept")

valid_courses = (
    df_courses
    .filter(F.col("dept") != "Biz")
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
    )
)

screened = (
    valid_courses.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.hour(F.col("e.enrolled_at")).between(6, 11)), "left_anti")
)

rollup_base = (
    screened
    .filter(F.lower(F.col("course_code")).like("%11%"))
    .groupBy("dept")
    .agg(
        F.round(F.stddev("credits"), 2).alias("stddev_credits"),
        F.array_sort(F.collect_set("dept")).alias("dept_list"),
        F.count(F.when(F.length(F.trim(F.col("course_code"))) >= 7, 1)).alias("n_length_trim_course_code_7"),
        F.sum("credits").alias("total_credits"),
        F.array_sort(F.collect_set("course_code")).alias("course_code_list"),
        F.count(F.when(F.upper(F.trim(F.col("course_code"))).like("A%"), 1)).alias("n_upper_trim_course_code_like"),
    )
)

ranked = (
    rollup_base
    .select(
        "*",
        F.lag("n_length_trim_course_code_7").over(w_all_by_total_credits).alias("prev_n_length_trim_course_code_7"),
        F.count(F.lit(1)).over(w_n_upper_trim_course_code_lik).alias("rows_in_n_upper_trim_course_code_like_dept"),
        F.rank().over(w_all_by_total_credits_ties).alias("total_credits_rank"),
    )
)

ranked_2 = (
    ranked
    .select(
        "*",
        F.count(F.lit(1)).over(w_total_credits_rank).alias("rows_in_total_credits_rank"),
        F.sum("total_credits").over(w_all_by_n_length_trim_course).alias("running_total_credits"),
        F.size(F.collect_set("dept").over(w_total_credits_rank)).alias("distinct_dept_in_total_credits_rank"),
        F.round(F.avg("total_credits_rank").over(w_total_credits_rank), 2).alias("total_credits_rank_avg_total_credits_rank"),
        F.ntile(3).over(w_all_by_prev_n_length_trim_co).alias("prev_n_length_trim_course_code_7_tercile"),
    )
)

result = (
    ranked_2
    .select(
        "dept",
        "stddev_credits",
        "n_length_trim_course_code_7",
        "total_credits",
        "course_code_list",
        "prev_n_length_trim_course_code_7",
        "rows_in_n_upper_trim_course_code_like_dept",
        "total_credits_rank",
        "rows_in_total_credits_rank",
        "running_total_credits",
        "distinct_dept_in_total_credits_rank",
        "total_credits_rank_avg_total_credits_rank",
        "prev_n_length_trim_course_code_7_tercile",
        F.lower(F.regexp_replace(F.trim(F.col("dept")), " ", "_")).alias("dept_slug"),
    )
)
