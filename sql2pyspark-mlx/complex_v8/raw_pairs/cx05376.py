from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_student_major = Window.partitionBy("grade_status", "student_major")

scoped_enrollments = (
    df_enrollments
    .filter(F.col("course_id") >= 5)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("term")), 1, 4).alias("term_prefix"),
        F.substring(F.trim(F.col("grade_status")), 1, 2).alias("grade_status_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

joined = (
    scoped_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.term_prefix",
        "s.grade_status_prefix",
        "s.term_slug",
        F.col("st.major").alias("student_major"),
        "st.student_name",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

ranked = (
    joined
    .select(
        "*",
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status_student_major), F.lit(0)), 4).alias("score_share_of_grade_status_student_major"),
        F.max("score").over(w_grade_status_student_major).alias("grade_status_student_major_max_score"),
        F.round(F.avg("score").over(w_grade_status_student_major), 2).alias("grade_status_student_major_avg_score"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status_student_major), 2).alias("score_vs_grade_status_student_major_avg"),
    )
)

result = (
    ranked
    .cube("student_major", "grade_status", "course_id")
    .agg(
        F.percentile_approx("grade_status_student_major_avg_score", 0.5).alias("median_grade_status_student_major_avg_score"),
        F.round(100 * F.count(F.when(F.col("grade_status").isin("WITHDRAWN", "INCOMPLETE", "FAIL"), 1)) / F.count("*"), 1).alias("pct_grade_status_in_withdrawn_in"),
        F.round(F.sum("grade_status_student_major_avg_score") / F.nullif(F.sum("score_share_of_grade_status_student_major"), F.lit(0)), 3).alias("grade_status_student_major_avg_score_to_score_sh"),
    )
    .orderBy(
        F.col("student_major").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
