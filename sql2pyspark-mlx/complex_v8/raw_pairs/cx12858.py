from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_level_course_dept_by = Window.partitionBy("course_level", "course_dept").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_level_course_dept_by_2 = Window.partitionBy("course_level", "course_dept").orderBy("enrolled_at")
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_level_course_dept_by_3 = Window.partitionBy("course_level", "course_dept").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_term_course_level_by_enrolle = Window.partitionBy("term", "course_level").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_term_course_level_by_enrolle_2 = Window.partitionBy("term", "course_level").orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_course_level = Window.partitionBy("course_level")
w_all_by_second_enrolled_at_mo = Window.orderBy("second_enrolled_at", "id").rowsBetween(-1, Window.currentRow)
w_all_by_second_enrolled_at_ru = Window.orderBy("second_enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_enrollments
    .filter((F.col("course_id") <= 9) & (F.col("score") >= 32.7))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
    )
)

joined = (
    base.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
    )
)

sequenced = (
    joined
    .select(
        "*",
        F.nth_value("enrolled_at", 2).over(w_course_level_course_dept_by).alias("second_enrolled_at"),
        F.round(F.percent_rank().over(w_course_level_course_dept_by_2), 3).alias("enrolled_at_percent_rank"),
        F.last("enrolled_at").over(w_all_by_enrolled_at_full).alias("last_enrolled_at"),
        F.last("enrolled_at", ignorenulls=True).over(w_course_level_course_dept_by_3).alias("last_known_enrolled_at"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_cume_dist"),
        F.last("enrolled_at").over(w_term_course_level_by_enrolle).alias("last_enrolled_at_2"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.round(F.cume_dist().over(w_term_course_level_by_enrolle_2), 3).alias("enrolled_at_cume_dist_2"),
        F.row_number().over(w_all_by_enrolled_at).alias("rn"),
        F.dense_rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_dense_rank"),
        F.row_number().over(w_term_course_level_by_enrolle_2).alias("rn_2"),
    )
)

first_rows = (
    sequenced
    .filter((F.col("rn_2") <= 2) & (F.col("course_id") >= 3))
    .drop("rn_2")
)

windowed = (
    first_rows
    .select(
        "id",
        "course_id",
        "term",
        "enrolled_at",
        "course_dept",
        "course_level",
        "last_enrolled_at",
        "enrolled_at_cume_dist",
        "last_enrolled_at_2",
        "minutes_since_prev_enrolled_at",
        "enrolled_at_cume_dist_2",
        "enrolled_at_dense_rank",
        F.count(F.lit(1)).over(w_course_level).alias("rows_in_course_level"),
        F.round(F.col("enrolled_at_cume_dist_2") - F.avg("enrolled_at_cume_dist_2").over(w_course_level), 2).alias("enrolled_at_cume_dist_2_vs_course_level_avg"),
        F.round(F.avg("enrolled_at_dense_rank").over(w_all_by_second_enrolled_at_mo), 2).alias("enrolled_at_dense_rank_avg_2"),
        F.max("enrolled_at_cume_dist").over(w_all_by_second_enrolled_at_ru).alias("max_enrolled_at_cume_dist_so_far"),
    )
)

result = (
    windowed
    .select(
        "id",
        "course_id",
        "term",
        "course_level",
        "last_enrolled_at",
        "enrolled_at_cume_dist",
        "last_enrolled_at_2",
        "minutes_since_prev_enrolled_at",
        "enrolled_at_cume_dist_2",
        "enrolled_at_dense_rank",
        "rows_in_course_level",
        "enrolled_at_cume_dist_2_vs_course_level_avg",
        "max_enrolled_at_cume_dist_so_far",
    )
    .orderBy("last_enrolled_at")
)
