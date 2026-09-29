from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_year_grade_statu = Window.partitionBy("enrolled_at_year", "grade_status").orderBy(F.col("score").desc())
w_enrolled_at_month_by_enrolle = Window.partitionBy(F.date_trunc("month", "enrolled_at")).orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_student_id = Window.partitionBy("term", "student_id")
w_enrolled_at_month_by_enrolle_2 = Window.partitionBy(F.date_trunc("month", "enrolled_at")).orderBy("enrolled_at", "id")

filtered_enrollments = (
    df_enrollments
    .filter(F.col("course_id") <= 6)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

ranked = (
    filtered_enrollments
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at_year",
        F.round(F.cume_dist().over(w_enrolled_at_year_grade_statu), 3).alias("score_cume_dist"),
        F.last("score_filled").over(w_enrolled_at_month_by_enrolle).alias("last_score_filled"),
        F.first("score").over(w_enrolled_at_month_by_enrolle).alias("first_score"),
        F.round(F.col("enrolled_at_year") / F.nullif(F.sum("enrolled_at_year").over(w_term_student_id), F.lit(0)), 4).alias("enrolled_at_year_share_of_term_student_id"),
        F.row_number().over(w_enrolled_at_month_by_enrolle_2).alias("seq_no"),
    )
)

with_activity = (
    ranked.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.gpa") < 3.2), "left_semi")
)

result = (
    with_activity
    .groupingSets(
        [["grade_status", "seq_no"], ["grade_status"], ["seq_no"], []],
        "grade_status", "seq_no",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.min("last_score_filled").alias("min_last_score_filled"),
    )
    .select(
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "seq_no",
        "grouping_level",
        "distinct_student_ids",
        "min_last_score_filled",
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("seq_no").asc_nulls_last(),
    )
)
