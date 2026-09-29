from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_course_id_by_sc = Window.partitionBy("grade_status", "course_id").orderBy("score_bucket", "id")
w_student_id_term_by_enrolled = Window.partitionBy("student_id", "term").orderBy("enrolled_at", "id")
w_course_id_grade_status = Window.partitionBy("course_id", "grade_status")
w_student_id_course_id_by_scor = Window.partitionBy("student_id", "course_id").orderBy(F.col("score_bucket").desc(), "id")
w_student_id_term_by_enrolled_2 = Window.partitionBy("student_id", "term").orderBy("enrolled_at")
w_term_by_rows_in_course_id_gr = Window.partitionBy("term").orderBy(F.col("rows_in_course_id_grade_status").desc(), "id")
w_prev_enrolled_at_month_term = Window.partitionBy(F.date_trunc("month", "prev_enrolled_at"), "term").orderBy("prev2_enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prev2_enrolled_at_month_term = Window.partitionBy(F.month("prev2_enrolled_at"), "term")
w_all_by_rn = Window.orderBy(F.col("rn").desc_nulls_first(), "id")

scoped_enrollments = (
    df_enrollments
    .filter(F.col("grade_status").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
    )
)

joined_course = (
    scoped_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_bucket",
        "c.course_code",
    )
)

sequenced = (
    joined_course
    .select(
        "*",
        F.ntile(3).over(w_grade_status_course_id_by_sc).alias("score_bucket_tercile"),
        F.ntile(4).over(w_student_id_term_by_enrolled).alias("enrolled_at_quartile"),
        F.max("score_bucket").over(w_course_id_grade_status).alias("course_id_grade_status_max_score_bucket"),
        F.row_number().over(w_student_id_course_id_by_scor).alias("seq_no"),
        F.size(F.collect_set("student_id").over(w_course_id_grade_status)).alias("distinct_student_id_in_course_id_grade_status"),
        F.round(F.percent_rank().over(w_student_id_term_by_enrolled_2), 3).alias("enrolled_at_percent_rank"),
        F.round(F.percent_rank().over(w_student_id_term_by_enrolled), 3).alias("enrolled_at_percent_rank_2"),
        F.count(F.lit(1)).over(w_course_id_grade_status).alias("rows_in_course_id_grade_status"),
        F.lag("enrolled_at", 2).over(w_student_id_term_by_enrolled).alias("prev2_enrolled_at"),
        F.lag("enrolled_at").over(w_student_id_course_id_by_scor).alias("prev_enrolled_at"),
        F.row_number().over(w_grade_status_course_id_by_sc).alias("rn"),
    )
)

top_per_group = sequenced.filter(F.col("seq_no") == 1)

metrics = (
    top_per_group
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "score_bucket",
        "enrolled_at_quartile",
        "course_id_grade_status_max_score_bucket",
        "distinct_student_id_in_course_id_grade_status",
        "enrolled_at_percent_rank_2",
        "rows_in_course_id_grade_status",
        "prev_enrolled_at",
        "rn",
        F.lead("enrolled_at").over(w_term_by_rows_in_course_id_gr).alias("next_enrolled_at"),
        F.last("prev2_enrolled_at").over(w_prev_enrolled_at_month_term).alias("last_prev2_enrolled_at"),
        F.round(F.avg("rn").over(w_prev2_enrolled_at_month_term), 2).alias("prev2_enrolled_at_month_term_avg_rn"),
        F.lead("prev_enrolled_at").over(w_all_by_rn).alias("next_prev_enrolled_at"),
    )
)

result = (
    metrics
    .select(
        "id",
        "student_id",
        "term",
        "score_bucket",
        "enrolled_at_quartile",
        "course_id_grade_status_max_score_bucket",
        "enrolled_at_percent_rank_2",
        "prev_enrolled_at",
        "rn",
        "next_enrolled_at",
        "last_prev2_enrolled_at",
        "prev2_enrolled_at_month_term_avg_rn",
        "next_prev_enrolled_at",
        (F.col("last_prev2_enrolled_at") + F.expr("INTERVAL 2 HOURS")).alias("last_prev2_enrolled_at_plus_2h"),
        (F.col("enrolled_at_quartile") - F.col("rn")).alias("enrolled_at_quartile_minus_rn"),
    )
    .orderBy(F.col("last_prev2_enrolled_at").asc_nulls_last())
)
