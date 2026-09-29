from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_major_grade_status_b = Window.partitionBy("student_major", "grade_status").orderBy(F.col("enrolled_at").desc())
w_student_major_grade_status_b_2 = Window.partitionBy("student_major", "grade_status").orderBy(F.col("enrolled_at").desc(), "id")
w_student_major_by_student_enr = Window.partitionBy("student_major").orderBy("student_enrolled_on", "id")
w_student_major_grade_status_b_3 = Window.partitionBy("student_major", "grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_enrolled_at_ran = Window.partitionBy("grade_status", "enrolled_at_rank").orderBy(F.col("second_student_enrolled_on").desc(), "id")
w_grade_status = Window.partitionBy("grade_status")

filtered_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2025T2")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

enriched_student = (
    filtered_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.*",
        F.col("st.major").alias("student_major"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

windowed = (
    enriched_student
    .select(
        "id",
        "term",
        "grade_status",
        "enrolled_at",
        "student_major",
        F.round(F.cume_dist().over(w_student_major_grade_status_b), 3).alias("enrolled_at_cume_dist"),
        F.lead("enrolled_at", 3).over(w_student_major_grade_status_b_2).alias("next3_enrolled_at"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_student_major_by_student_enr))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.dense_rank().over(w_student_major_grade_status_b).alias("enrolled_at_dense_rank"),
        F.nth_value("student_enrolled_on", 2).over(w_student_major_grade_status_b_3).alias("second_student_enrolled_on"),
        F.rank().over(w_student_major_grade_status_b).alias("enrolled_at_rank"),
    )
)

metrics = (
    windowed
    .select(
        "id",
        "term",
        "enrolled_at",
        "student_major",
        "enrolled_at_cume_dist",
        "next3_enrolled_at",
        "minutes_since_prev_enrolled_at",
        "enrolled_at_dense_rank",
        "second_student_enrolled_on",
        "enrolled_at_rank",
        F.ntile(5).over(w_grade_status_enrolled_at_ran).alias("second_student_enrolled_on_quintile"),
        F.round(F.avg("enrolled_at_rank").over(w_grade_status), 2).alias("grade_status_avg_enrolled_at_rank"),
        F.ntile(3).over(w_grade_status_enrolled_at_ran).alias("second_student_enrolled_on_tercile"),
    )
)

result = (
    metrics
    .groupingSets(
        [["second_student_enrolled_on_quintile", "enrolled_at_rank"], ["second_student_enrolled_on_quintile"], []],
        "second_student_enrolled_on_quintile", "enrolled_at_rank",
    )
    .agg(
        F.datediff(F.max("second_student_enrolled_on"), F.min("second_student_enrolled_on")).alias("second_student_enrolled_on_span_days"),
    )
)
