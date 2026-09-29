from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_is_international_enr = Window.partitionBy("student_is_international", "enrolled_at_year")
w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_all_by_score = Window.orderBy("score", "id")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")

valid_enrollments = (
    df_enrollments
    .filter(F.col("course_id") <= 8)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
    )
)

joined_student = (
    valid_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(~F.col("st.major").isin("Art", "Biz"))
    .select(
        "s.*",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.major").alias("student_major"),
    )
)

sequenced = (
    joined_student
    .select(
        "*",
        F.round(F.col("enrolled_at_year") / F.nullif(F.sum("enrolled_at_year").over(w_student_is_international_enr), F.lit(0)), 4).alias("enrolled_at_year_share_of_student_is_internation"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_score), 3).alias("score_cume_dist"),
        F.ntile(10).over(w_all_by_enrolled_at).alias("enrolled_at_decile"),
        F.size(F.collect_set("student_id").over(w_student_is_international_enr)).alias("distinct_student_id_in_student_is_international_"),
    )
)

derived = (
    sequenced
    .select(
        "*",
        F.concat_ws(": ", F.col("grade_status"), F.col("student_major")).alias("grade_status_student_major_label"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref"),
    )
)

enriched = (
    derived.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.lower(F.col("c.course_code")).like("%ma%")), "inner")
    .filter(F.col("c.level").isNotNull())
    .select("s.*", F.col("c.level").alias("course_level"))
)

joined_student_2 = (
    enriched.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        "s.enrolled_at_year",
        "s.student_is_international",
        "s.student_major",
        "s.enrolled_at_year_share_of_student_is_internation",
        "s.score_cume_dist",
        "s.enrolled_at_decile",
        "s.distinct_student_id_in_student_is_international_",
        "s.grade_status_ref",
        "s.course_level",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.major").alias("student_major_2"),
        "st.student_name",
        F.col("c.dept").alias("course_dept"),
    )
)

annotated = (
    joined_student_2
    .select(
        "*",
        F.year(F.col("student_enrolled_on")).alias("student_enrolled_on_year"),
        F.greatest(F.col("enrolled_at_year"), F.col("enrolled_at_decile")).alias("greatest_enrolled_at_year_enrolled_at_decile"),
        F.greatest(F.col("enrolled_at_decile"), F.col("score_cume_dist")).alias("greatest_enrolled_at_decile_score_cume_dist"),
    )
)

result = (
    annotated
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at_year",
        "student_major",
        "score_cume_dist",
        "distinct_student_id_in_student_is_international_",
        "grade_status_ref",
        "course_level",
        "student_enrolled_on",
        "student_major_2",
        "student_name",
        "course_dept",
        "student_enrolled_on_year",
        "greatest_enrolled_at_year_enrolled_at_decile",
        "greatest_enrolled_at_decile_score_cume_dist",
        F.trunc(F.col("student_enrolled_on"), "MM").alias("student_enrolled_on_month_start"),
        F.abs(F.col("score_cume_dist") - 0.6).alias("score_cume_dist_distance"),
    )
    .orderBy("distinct_student_id_in_student_is_international_")
)
