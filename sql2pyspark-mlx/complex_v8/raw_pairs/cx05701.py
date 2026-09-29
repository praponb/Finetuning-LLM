from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_major_by_student_enr = Window.partitionBy("student_major").orderBy(F.col("student_enrolled_on").asc_nulls_last(), "id")
w_enrolled_at_month_student_id = Window.partitionBy(F.month("enrolled_at"), "student_id").orderBy("student_enrolled_on", "id")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_enrolled_at_month_student_id_2 = Window.partitionBy(F.month("enrolled_at"), "student_id").orderBy("student_enrolled_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_full = Window.orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_month_student_id_3 = Window.partitionBy(F.month("enrolled_at"), "student_id").orderBy("student_enrolled_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_prev_enrolled_at_plus = Window.orderBy("prev_enrolled_at_plus_15m", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_score_tier_grade_status_by_e = Window.partitionBy("score_tier", "grade_status").orderBy(F.col("enrolled_at_cume_dist").asc_nulls_last())
w_all_by_prev_enrolled_at_plus_2 = Window.orderBy("prev_enrolled_at_plus_15m", "id")
w_all_by_enrolled_at_plus_15m = Window.orderBy("enrolled_at_plus_15m", "id")
w_student_enrolled_on_dense_ra = Window.partitionBy("student_enrolled_on_dense_rank", "course_id")
w_all_by_n_grade_status_incomp = Window.orderBy(F.col("n_grade_status_incomplete").desc(), "prev_enrolled_at_plus_15m_day", "course_id", "student_enrolled_on_dense_rank")
w_all_by_n_grade_status_incomp_2 = Window.orderBy(F.col("n_grade_status_incomplete").desc(), "prev_enrolled_at_plus_15m_day", "course_id", "student_enrolled_on_dense_rank").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.hour(F.col("enrolled_at")).between(10, 15)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        (F.col("enrolled_at") + F.expr("INTERVAL 15 MINUTES")).alias("enrolled_at_plus_15m"),
        F.when(F.col("score") >= 57.8, "large").when(F.col("score") >= 20.6, "medium").otherwise("small").alias("score_tier"),
    )
)

with_parent = (
    valid_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_plus_15m",
        "s.score_tier",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.major").alias("student_major"),
        F.col("c.dept").alias("course_dept"),
    )
)

metrics = (
    with_parent
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "enrolled_at_plus_15m",
        "score_tier",
        "student_enrolled_on",
        "student_major",
        F.lag("student_enrolled_on").over(w_student_major_by_student_enr).alias("prev_student_enrolled_on"),
        F.row_number().over(w_student_major_by_student_enr).alias("rn"),
        F.lag("enrolled_at_plus_15m").over(w_enrolled_at_month_student_id).alias("prev_enrolled_at_plus_15m"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at), 3).alias("enrolled_at_cume_dist"),
        F.last("student_enrolled_on", ignorenulls=True).over(w_enrolled_at_month_student_id_2).alias("last_known_student_enrolled_on"),
        F.nth_value("enrolled_at_plus_15m", 2).over(w_all_by_enrolled_at_full).alias("second_enrolled_at_plus_15m"),
        F.round(F.percent_rank().over(w_enrolled_at_month_student_id), 3).alias("student_enrolled_on_percent_rank"),
        F.first("enrolled_at_plus_15m").over(w_all_by_enrolled_at_full).alias("first_enrolled_at_plus_15m"),
        F.datediff(F.col("student_enrolled_on"), F.lag("student_enrolled_on").over(w_student_major_by_student_enr)).alias("days_since_prev_student_enrolled_on"),
        F.dense_rank().over(w_enrolled_at_month_student_id).alias("student_enrolled_on_dense_rank"),
        F.first("student_enrolled_on").over(w_enrolled_at_month_student_id_3).alias("first_student_enrolled_on"),
    )
)

enriched = (
    metrics
    .filter(F.col("enrolled_at") < F.lit("2025-02-12 09:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "enrolled_at_plus_15m",
        "score_tier",
        "prev_enrolled_at_plus_15m",
        "enrolled_at_cume_dist",
        "last_known_student_enrolled_on",
        "second_enrolled_at_plus_15m",
        "days_since_prev_student_enrolled_on",
        "student_enrolled_on_dense_rank",
        F.to_date(F.col("enrolled_at_plus_15m")).alias("enrolled_at_plus_15m_date"),
        F.greatest(F.col("enrolled_at_cume_dist"), F.col("rn")).alias("greatest_enrolled_at_cume_dist_rn"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.last("enrolled_at_cume_dist").over(w_all_by_prev_enrolled_at_plus).alias("last_enrolled_at_cume_dist"),
        F.dense_rank().over(w_score_tier_grade_status_by_e).alias("enrolled_at_cume_dist_dense_rank"),
        F.dense_rank().over(w_all_by_prev_enrolled_at_plus_2).alias("prev_enrolled_at_plus_15m_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at_plus_15m), 3).alias("enrolled_at_plus_15m_cume_dist"),
    )
)

stats = (
    ranked
    .filter(F.col("days_since_prev_student_enrolled_on") >= 70)
    .groupBy(
        F.to_date("prev_enrolled_at_plus_15m").alias("prev_enrolled_at_plus_15m_day"),
        "course_id",
        "student_enrolled_on_dense_rank",
    )
    .agg(
        F.datediff(F.max("last_known_student_enrolled_on"), F.min("last_known_student_enrolled_on")).alias("last_known_student_enrolled_on_span_days"),
        F.sum(F.when(F.col("grade_status") == "INCOMPLETE", 1).otherwise(0)).alias("n_grade_status_incomplete"),
        F.sum(F.when(F.col("grade_status") == "WITHDRAWN", F.col("days_since_prev_student_enrolled_on"))).alias("days_since_prev_student_enrolled_on_grade_status"),
        F.max("enrolled_at_plus_15m_cume_dist").alias("max_enrolled_at_plus_15m_cume_dist"),
    )
)

ranked_2 = (
    stats
    .select(
        "prev_enrolled_at_plus_15m_day",
        "course_id",
        "student_enrolled_on_dense_rank",
        "n_grade_status_incomplete",
        "days_since_prev_student_enrolled_on_grade_status",
        "max_enrolled_at_plus_15m_cume_dist",
        F.count(F.lit(1)).over(w_student_enrolled_on_dense_ra).alias("rows_in_student_enrolled_on_dense_rank_course_id"),
        F.ntile(3).over(w_all_by_n_grade_status_incomp).alias("n_grade_status_incomplete_tercile"),
        F.sum("n_grade_status_incomplete").over(w_all_by_n_grade_status_incomp_2).alias("running_n_grade_status_incomplete"),
        F.sum("n_grade_status_incomplete").over(w_student_enrolled_on_dense_ra).alias("student_enrolled_on_dense_rank_course_id_sum_n_g"),
    )
)

result = (
    ranked_2
    .select(
        "prev_enrolled_at_plus_15m_day",
        "course_id",
        "student_enrolled_on_dense_rank",
        "n_grade_status_incomplete",
        "days_since_prev_student_enrolled_on_grade_status",
        "max_enrolled_at_plus_15m_cume_dist",
        "running_n_grade_status_incomplete",
    )
    .orderBy(F.col("course_id").desc())
)
