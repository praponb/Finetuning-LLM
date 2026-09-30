from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_major_course_id = Window.partitionBy("student_major", "course_id")
w_course_id_by_score_bucket = Window.partitionBy("course_id").orderBy(F.col("score_bucket").desc_nulls_first(), "id")

cleaned = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("day", F.col("enrolled_at")).alias("enrolled_at_day"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
    )
)

combined_details = (
    cleaned.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select("s.*", "st.student_name", F.col("st.major").alias("student_major"))
)

with_windows = (
    combined_details
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "term_prefix",
        "student_name",
        "student_major",
        F.round(F.col("score_bucket") / F.nullif(F.sum("score_bucket").over(w_student_major_course_id), F.lit(0)), 4).alias("score_bucket_share_of_student_major_course_id"),
        F.count(F.lit(1)).over(w_student_major_course_id).alias("rows_in_student_major_course_id"),
        (F.col("score_bucket") - F.lag("score_bucket").over(w_course_id_by_score_bucket)).alias("score_bucket_change"),
        F.ntile(4).over(w_course_id_by_score_bucket).alias("score_bucket_quartile"),
        F.lag("score", 2).over(w_course_id_by_score_bucket).alias("prev2_score"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "term_prefix",
        "student_name",
        "student_major",
        "score_bucket_share_of_student_major_course_id",
        "rows_in_student_major_course_id",
        "score_bucket_change",
        "score_bucket_quartile",
        "prev2_score",
    )
    .orderBy("term_prefix", F.col("student_name").desc())
)
