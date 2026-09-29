from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_dept_enrolled_at_mont = Window.partitionBy("course_dept", "enrolled_at_month").orderBy(F.col("days_since_enrolled_at").desc_nulls_first(), "id")
w_student_is_international_by = Window.partitionBy("student_is_international").orderBy("days_since_enrolled_at", "id")
w_student_enrolled_on_year_enr = Window.partitionBy(F.year("student_enrolled_on"), "enrolled_at_month")
w_student_is_international_by_2 = Window.partitionBy("student_is_international").orderBy("days_since_enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term = Window.partitionBy("term")
w_student_id_by_score_running = Window.partitionBy("student_id").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_by_score_ties = Window.partitionBy("student_id").orderBy(F.col("score").desc())
w_term_course_dept_by_enrolled = Window.partitionBy("term", "course_dept").orderBy("enrolled_at_month", "id")
w_distinct_course_id_in_term_b = Window.partitionBy("distinct_course_id_in_term").orderBy("rows_in_student_enrolled_on_year_enrolled_at_mon", "id")
w_distinct_course_id_in_term_b_2 = Window.partitionBy("distinct_course_id_in_term").orderBy("rows_in_student_enrolled_on_year_enrolled_at_mon", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_student_enrolled_on_t = Window.orderBy("student_enrolled_on")
w_student_is_international = Window.partitionBy("student_is_international")

prepared = (
    df_enrollments
    .filter(F.col("course_id") >= 11)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at_month",
        "s.days_since_enrolled_at",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("c.dept").alias("course_dept"),
    )
)

with_windows = (
    with_parent
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at_month",
        "days_since_enrolled_at",
        "student_is_international",
        "student_enrolled_on",
        F.round(F.percent_rank().over(w_course_dept_enrolled_at_mont), 3).alias("days_since_enrolled_at_percent_rank"),
        F.datediff(F.col("student_enrolled_on"), F.lag("student_enrolled_on").over(w_student_is_international_by)).alias("days_since_prev_student_enrolled_on"),
        F.round(F.col("enrolled_at_month") / F.nullif(F.sum("enrolled_at_month").over(w_student_enrolled_on_year_enr), F.lit(0)), 4).alias("enrolled_at_month_share_of_student_enrolled_on_y"),
        F.row_number().over(w_student_is_international_by).alias("days_since_enrolled_at_seq"),
        F.count(F.lit(1)).over(w_student_is_international_by_2).alias("running_count"),
        F.size(F.collect_set("course_id").over(w_term)).alias("distinct_course_id_in_term"),
        F.max("days_since_enrolled_at").over(w_student_id_by_score_running).alias("max_days_since_enrolled_at_so_far"),
        F.rank().over(w_student_id_by_score_ties).alias("score_rank"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.lead("enrolled_at_month").over(w_term_course_dept_by_enrolled).alias("next_enrolled_at_month"),
        F.count(F.lit(1)).over(w_student_enrolled_on_year_enr).alias("rows_in_student_enrolled_on_year_enrolled_at_mon"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at_month",
        "days_since_enrolled_at",
        "student_is_international",
        "student_enrolled_on",
        "days_since_enrolled_at_percent_rank",
        "days_since_prev_student_enrolled_on",
        "enrolled_at_month_share_of_student_enrolled_on_y",
        "days_since_enrolled_at_seq",
        "running_count",
        "distinct_course_id_in_term",
        "max_days_since_enrolled_at_so_far",
        "score_rank",
        "next_enrolled_at_month",
        "rows_in_student_enrolled_on_year_enrolled_at_mon",
        F.round(F.cume_dist().over(w_distinct_course_id_in_term_b), 3).alias("rows_in_student_enrolled_on_year_enrolled_at_mon_2"),
        F.nth_value("student_enrolled_on", 2).over(w_distinct_course_id_in_term_b_2).alias("second_student_enrolled_on"),
        F.round(F.cume_dist().over(w_all_by_student_enrolled_on_t), 3).alias("student_enrolled_on_cume_dist"),
        F.size(F.collect_set("term").over(w_student_is_international)).alias("distinct_term_in_student_is_international"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "days_since_enrolled_at",
        "days_since_enrolled_at_percent_rank",
        "days_since_prev_student_enrolled_on",
        "enrolled_at_month_share_of_student_enrolled_on_y",
        "days_since_enrolled_at_seq",
        "running_count",
        "distinct_course_id_in_term",
        "max_days_since_enrolled_at_so_far",
        "score_rank",
        "next_enrolled_at_month",
        "rows_in_student_enrolled_on_year_enrolled_at_mon_2",
        "second_student_enrolled_on",
        "distinct_term_in_student_is_international",
    )
    .orderBy("running_count", "max_days_since_enrolled_at_so_far")
)
