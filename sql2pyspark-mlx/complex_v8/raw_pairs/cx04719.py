from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id")
w_term_group_student_is_intern = Window.partitionBy("term_group", "student_is_international").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_term_group = Window.partitionBy("student_id", "term_group")
w_student_id_course_id_by_enro = Window.partitionBy("student_id", "course_id").orderBy("enrolled_at", "id").rowsBetween(-6, -1)
w_term_group_student_is_intern_2 = Window.partitionBy("term_group", "student_is_international").orderBy("enrolled_at")
w_all_by_student_enrolled_on = Window.orderBy("student_enrolled_on", "id")
w_all_by_score = Window.orderBy("score", "id")
w_student_id_course_id_by_enro_2 = Window.partitionBy("student_id", "course_id").orderBy("enrolled_at")
w_all_by_score_ties = Window.orderBy("score")
w_term_group_student_is_intern_3 = Window.partitionBy("term_group", "student_is_international").orderBy("enrolled_at", "id")
w_student_id_course_id_by_enro_3 = Window.partitionBy("student_id", "course_id").orderBy("enrolled_at", "id")
w_last_student_enrolled_on_yea = Window.partitionBy("last_student_enrolled_on_year")
w_all_by_score_share_of_studen = Window.orderBy("score_share_of_student_id_term_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_term_in_course_id_r = Window.partitionBy("distinct_term_in_course_id", "rn").orderBy(F.col("last_student_enrolled_on_year").desc())

cleaned_enrollments = (
    df_enrollments
    .filter((~F.col("grade_status").isin("PASS", "INCOMPLETE")) & (F.col("term") == "2024T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.when(F.col("term").isin("2024T2", "2025T2"), "group_a").when(F.col("term") == "2024T1", "group_b").otherwise("other").alias("term_group"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref"),
    )
)

joined = (
    cleaned_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.term_group",
        "s.grade_status_ref",
        F.col("st.major").alias("student_major"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

windowed = (
    joined
    .select(
        "id",
        "course_id",
        "term",
        "term_group",
        "grade_status_ref",
        "student_major",
        "student_enrolled_on",
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
        F.size(F.collect_set("term_group").over(w_course_id)).alias("distinct_term_group_in_course_id"),
        F.last("student_enrolled_on").over(w_term_group_student_is_intern).alias("last_student_enrolled_on"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id_term_group), F.lit(0)), 4).alias("score_share_of_student_id_term_group"),
        F.round(F.avg("score").over(w_student_id_course_id_by_enro), 2).alias("score_avg_6_prior"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.rank().over(w_term_group_student_is_intern_2).alias("enrolled_at_rank"),
        F.size(F.collect_set("term").over(w_course_id)).alias("distinct_term_in_course_id"),
        F.lag("score").over(w_all_by_student_enrolled_on).alias("prev_score"),
        F.lag("student_enrolled_on").over(w_all_by_score).alias("prev_student_enrolled_on"),
        F.round(F.percent_rank().over(w_student_id_course_id_by_enro_2), 3).alias("enrolled_at_percent_rank"),
        F.rank().over(w_all_by_score_ties).alias("score_rank"),
        F.row_number().over(w_term_group_student_is_intern_3).alias("rn"),
        F.row_number().over(w_student_id_course_id_by_enro_3).alias("enrolled_at_seq"),
    )
)

tagged = (
    windowed
    .select(
        "id",
        "term_group",
        "student_major",
        "student_enrolled_on",
        "score_share_of_course_id",
        "score_share_of_student_id_term_group",
        "score_avg_6_prior",
        "rows_in_course_id",
        "enrolled_at_rank",
        "distinct_term_in_course_id",
        "prev_student_enrolled_on",
        "enrolled_at_percent_rank",
        "rn",
        "enrolled_at_seq",
        F.round(F.col("rows_in_course_id") / F.nullif(F.col("prev_score"), F.lit(0)), 3).alias("rows_in_course_id_per_prev_score"),
        F.year(F.col("last_student_enrolled_on")).alias("last_student_enrolled_on_year"),
    )
)

ranked = (
    tagged
    .select(
        "id",
        "term_group",
        "student_major",
        "student_enrolled_on",
        "score_share_of_course_id",
        "score_avg_6_prior",
        "rows_in_course_id",
        "enrolled_at_rank",
        "distinct_term_in_course_id",
        "prev_student_enrolled_on",
        "rn",
        "enrolled_at_seq",
        "rows_in_course_id_per_prev_score",
        "last_student_enrolled_on_year",
        F.size(F.collect_set("term_group").over(w_last_student_enrolled_on_yea)).alias("distinct_term_group_in_last_student_enrolled_on_"),
        F.sum("enrolled_at_seq").over(w_all_by_score_share_of_studen).alias("running_enrolled_at_seq"),
        F.round(F.cume_dist().over(w_distinct_term_in_course_id_r), 3).alias("last_student_enrolled_on_year_cume_dist"),
    )
)

result = (
    ranked
    .filter(F.col("enrolled_at_rank").between(2, 7))
    .select("enrolled_at_seq")
    .distinct()
    .orderBy(F.col("enrolled_at_seq").asc_nulls_last())
)
