from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id")
w_student_id_course_id_by_enro = Window.partitionBy("student_id", "course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_term_by_score_mov = Window.partitionBy("student_id", "term").orderBy(F.col("score").desc(), "id").rowsBetween(-1, Window.currentRow)
w_student_id_term_by_score = Window.partitionBy("student_id", "term").orderBy(F.col("score").desc(), "id")
w_term_by_score = Window.partitionBy("term").orderBy(F.col("score").desc(), "id")
w_grade_status_enrolled_at_to = Window.partitionBy("grade_status", F.to_date("enrolled_at")).orderBy(F.col("score").asc_nulls_last(), "id")
w_distinct_student_major_in_co = Window.partitionBy("distinct_student_major_in_course_id", "grade_status")
w_enrolled_at_month_grade_stat = Window.partitionBy(F.month("enrolled_at"), "grade_status").orderBy(F.col("score_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_month_grade_stat_2 = Window.partitionBy(F.month("enrolled_at"), "grade_status").orderBy(F.col("score_cume_dist").desc(), "id")
w_enrolled_at_month_grade_stat_3 = Window.partitionBy(F.month("enrolled_at"), "grade_status").orderBy(F.col("score_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_enrollments
    .filter((F.col("course_id") <= 9) & (~F.col("term").isin("2025T2", "2025T1")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_parent = (
    valid.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (~F.col("st.is_international")), "left")
    .select("s.*", "st.student_name", F.col("st.major").alias("student_major"))
)

windowed = (
    with_parent
    .select(
        "*",
        F.round(F.col("score") - F.avg("score").over(w_course_id), 2).alias("score_vs_course_id_avg"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.min("score").over(w_course_id).alias("course_id_min_score"),
        F.first("enrolled_at", ignorenulls=True).over(w_student_id_course_id_by_enro).alias("first_known_enrolled_at"),
        F.sum("score").over(w_student_id_term_by_score_mov).alias("score_sum_2"),
        F.size(F.collect_set("student_major").over(w_course_id)).alias("distinct_student_major_in_course_id"),
        F.size(F.collect_set("grade_status").over(w_course_id)).alias("distinct_grade_status_in_course_id"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_student_id_term_by_score))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.round(F.cume_dist().over(w_term_by_score), 3).alias("score_cume_dist"),
        F.row_number().over(w_grade_status_enrolled_at_to).alias("rn"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
    )
)

ranked = (
    windowed
    .select(
        "*",
        F.count(F.lit(1)).over(w_distinct_student_major_in_co).alias("rows_in_distinct_student_major_in_course_id_grad"),
        F.sum("score_vs_course_id_avg").over(w_distinct_student_major_in_co).alias("distinct_student_major_in_course_id_grade_status"),
        F.max("score_vs_course_id_avg").over(w_distinct_student_major_in_co).alias("distinct_student_major_in_course_id_grade_status_2"),
        F.last("enrolled_at").over(w_enrolled_at_month_grade_stat).alias("last_enrolled_at"),
        F.lead("rn", 3).over(w_enrolled_at_month_grade_stat_2).alias("next3_rn"),
        F.row_number().over(w_enrolled_at_month_grade_stat_2).alias("seq_no"),
        F.last("score_sum_2", ignorenulls=True).over(w_enrolled_at_month_grade_stat_3).alias("last_known_score_sum_2"),
    )
)

result = (
    ranked
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "student_name",
        "student_major",
        "score_vs_course_id_avg",
        "rows_in_course_id",
        "course_id_min_score",
        "score_sum_2",
        "distinct_student_major_in_course_id",
        "score_cume_dist",
        "score_share_of_course_id",
        "rows_in_distinct_student_major_in_course_id_grad",
        "distinct_student_major_in_course_id_grade_status_2",
        "last_enrolled_at",
        "next3_rn",
        "seq_no",
        "last_known_score_sum_2",
    )
    .orderBy(F.col("distinct_student_major_in_course_id_grade_status_2").desc())
)
