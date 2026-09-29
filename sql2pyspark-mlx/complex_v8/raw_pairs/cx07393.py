from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at_quarter = Window.orderBy(F.col("enrolled_at_quarter").desc(), "id")
w_grade_status = Window.partitionBy("grade_status")
w_all_by_enrolled_at_quarter_f = Window.orderBy(F.col("enrolled_at_quarter").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_filled_full = Window.orderBy(F.col("score_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_enrollments = (
    df_enrollments
    .filter(F.col("score") > 11.3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("WITHDRAWN", "PASS"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

enriched = (
    valid_enrollments
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

combined_details = (
    enriched.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.student_name").isNotNull()), "inner")
    .select(
        "s.*",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.major").alias("student_major"),
    )
)

sequenced = (
    combined_details
    .select(
        "*",
        F.lead("score_filled", 3).over(w_all_by_enrolled_at_quarter).alias("next3_score_filled"),
        F.lead("grade_status_len", 1, 0).over(w_all_by_enrolled_at_quarter).alias("next_grade_status_len"),
        F.size(F.collect_set("student_major").over(w_grade_status)).alias("distinct_student_major_in_grade_status"),
        F.first("score_filled").over(w_all_by_enrolled_at_quarter_f).alias("first_score_filled"),
        F.nth_value("score_filled", 2).over(w_all_by_score_filled_full).alias("second_score_filled"),
        F.round(F.col("enrolled_at_quarter") / F.nullif(F.sum("enrolled_at_quarter").over(w_grade_status), F.lit(0)), 4).alias("enrolled_at_quarter_share_of_grade_status"),
    )
)

combined_details_2 = (
    sequenced.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.is_international")), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_len",
        "s.score_filled",
        "s.student_cohort",
        "s.student_is_international",
        "s.next_grade_status_len",
        "s.first_score_filled",
        "s.enrolled_at_quarter_share_of_grade_status",
        F.col("st.cohort").alias("student_cohort_2"),
        F.col("st.major").alias("student_major_2"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

result = (
    combined_details_2
    .select("id", "next_grade_status_len", "grade_status_len")
    .unpivot(
        ids=["id"],
        values=["next_grade_status_len", "grade_status_len"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
