from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_enrolled_at = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_course_dept_by_enr = Window.partitionBy("course_id", "course_dept").orderBy("enrolled_at", "id")
w_course_dept_student_id_by_en = Window.partitionBy("course_dept", "student_id").orderBy("enrolled_at", "id")
w_course_dept_by_enrolled_at = Window.partitionBy("course_dept").orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_all_by_enrolled_at_running = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_dept_student_id_by_en_2 = Window.partitionBy("course_dept", "student_id").orderBy("enrolled_at")
w_course_id_course_dept_by_enr_2 = Window.partitionBy("course_id", "course_dept").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_dept_by_enrolled_at_t = Window.partitionBy("course_dept").orderBy(F.col("enrolled_at").asc_nulls_last())

valid_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) | (F.col("grade_status").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

joined_course = (
    valid_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.dept") != "CS")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        F.col("c.dept").alias("course_dept"),
    )
)

metrics = (
    joined_course
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "course_dept",
        F.lag("enrolled_at").over(w_course_id_by_enrolled_at).alias("prev_enrolled_at"),
        F.round(F.cume_dist().over(w_course_id_course_dept_by_enr), 3).alias("enrolled_at_cume_dist"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_course_dept_student_id_by_en))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.lead("enrolled_at", 3).over(w_course_dept_by_enrolled_at).alias("next3_enrolled_at"),
        F.lag("enrolled_at").over(w_all_by_enrolled_at).alias("prev_enrolled_at_2"),
        F.last("enrolled_at", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("last_known_enrolled_at"),
        F.dense_rank().over(w_course_dept_student_id_by_en_2).alias("enrolled_at_dense_rank"),
        F.ntile(5).over(w_all_by_enrolled_at).alias("enrolled_at_quintile"),
        F.dense_rank().over(w_all_by_enrolled_at).alias("enrolled_at_dense_rank_2"),
        F.last("enrolled_at").over(w_course_id_course_dept_by_enr_2).alias("last_enrolled_at"),
        F.lag("enrolled_at", 3).over(w_course_id_by_enrolled_at).alias("prev3_enrolled_at"),
        F.dense_rank().over(w_course_dept_by_enrolled_at_t).alias("enrolled_at_dense_rank_3"),
    )
)

summary = (
    metrics
    .filter(F.col("enrolled_at_cume_dist") >= 0.1)
    .groupBy(
        F.date_format("prev_enrolled_at", "yyyy-MM").alias("prev_enrolled_at_ym"),
        F.date_trunc("month", "prev_enrolled_at_2").alias("prev_enrolled_at_2_month"),
        F.date_trunc("month", "next3_enrolled_at").alias("next3_enrolled_at_month"),
    )
    .agg(
        F.min("prev_enrolled_at_2").alias("first_prev_enrolled_at_2"),
        F.sum(F.when(F.col("course_dept").isNotNull(), 1).otherwise(0)).alias("n_course_dept_is_not_null"),
        F.percentile_approx("enrolled_at_dense_rank_3", 0.5).alias("median_enrolled_at_dense_rank_3"),
        F.round(F.stddev("enrolled_at_quintile"), 2).alias("stddev_enrolled_at_quintile"),
    )
)

result = (
    summary
    .filter(F.col("median_enrolled_at_dense_rank_3") <= 9)
    .select(
        "prev_enrolled_at_ym",
        "next3_enrolled_at_month",
        "first_prev_enrolled_at_2",
        "n_course_dept_is_not_null",
        "stddev_enrolled_at_quintile",
    )
    .orderBy(
        "next3_enrolled_at_month",
        F.col("prev_enrolled_at_ym").desc_nulls_first(),
    )
)
