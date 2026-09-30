from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_credits_by_enrolled_a = Window.partitionBy("course_credits").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_credits = Window.partitionBy("course_credits")
w_term = Window.partitionBy("term")
w_course_id_by_enrolled_at = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_course_credits = Window.orderBy(F.col("course_credits").desc(), "id")

cleaned = (
    df_enrollments
    .filter((F.col("student_id") >= 4) & (F.month(F.col("enrolled_at")).isin(2, 3)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

joined_course = (
    cleaned.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.grade_status_or_unknown",
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
    )
)

with_windows = (
    joined_course
    .select(
        "*",
        F.count(F.lit(1)).over(w_course_credits_by_enrolled_a).alias("running_count"),
        F.round(F.col("course_credits") - F.avg("course_credits").over(w_course_credits), 2).alias("course_credits_vs_course_credits_avg"),
        F.size(F.collect_set("term").over(w_course_credits)).alias("distinct_term_in_course_credits"),
        F.size(F.collect_set("course_dept").over(w_term)).alias("distinct_course_dept_in_term"),
        F.lag("course_credits", 3).over(w_course_id_by_enrolled_at).alias("prev3_course_credits"),
        F.count(F.lit(1)).over(w_course_credits).alias("rows_in_course_credits"),
        F.min("course_credits").over(w_term).alias("term_min_course_credits"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_course_credits))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.sum("course_credits").over(w_term).alias("term_sum_course_credits"),
        F.max("course_credits").over(w_term).alias("term_max_course_credits"),
        F.row_number().over(w_course_id_by_enrolled_at).alias("seq_no"),
        F.min("course_credits").over(w_course_credits).alias("course_credits_min_course_credits"),
    )
)

result = (
    with_windows
    .filter(F.col("course_credits_min_course_credits") <= 32)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "grade_status_or_unknown",
        "course_dept",
        "course_credits",
        "course_code",
        "running_count",
        "course_credits_vs_course_credits_avg",
        "distinct_term_in_course_credits",
        "distinct_course_dept_in_term",
        "prev3_course_credits",
        "rows_in_course_credits",
        "term_min_course_credits",
        "term_sum_course_credits",
        "term_max_course_credits",
        "seq_no",
        F.length(F.trim(F.col("grade_status_or_unknown"))).alias("grade_status_or_unknown_len"),
        (F.col("term_sum_course_credits") + F.col("running_count")).alias("term_sum_course_credits_plus_running_count"),
    )
    .orderBy("term", "course_id")
)
