from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_student_is_interna = Window.partitionBy("course_id", "student_is_international").orderBy("score", "id")
w_course_id_by_score_ties = Window.partitionBy("course_id").orderBy("score")
w_course_id = Window.partitionBy("course_id")
w_course_id_student_is_interna_2 = Window.partitionBy("course_id", "student_is_international").orderBy("score")
w_all_by_score = Window.orderBy(F.col("score").desc_nulls_first(), "id")
w_course_id_by_score = Window.partitionBy("course_id").orderBy("score", "id")
w_student_is_international = Window.partitionBy("student_is_international")
w_student_is_international_cou = Window.partitionBy("student_is_international", "course_id").orderBy("enrolled_at", "id")

recent = (
    df_enrollments
    .filter(F.col("course_id") >= 3)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
    )
)

with_parent = (
    recent.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.is_international"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_prefix",
        "st.student_name",
        F.col("st.is_international").alias("student_is_international"),
    )
)

with_trends = (
    with_parent
    .select(
        "*",
        F.lag("enrolled_at", 3).over(w_course_id_student_is_interna).alias("prev3_enrolled_at"),
        F.round(F.percent_rank().over(w_course_id_by_score_ties), 3).alias("score_percent_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
        F.rank().over(w_course_id_student_is_interna_2).alias("score_rank"),
        F.rank().over(w_all_by_score).alias("score_rank_2"),
        F.lead("enrolled_at").over(w_course_id_by_score).alias("next_enrolled_at"),
        F.count(F.lit(1)).over(w_student_is_international).alias("rows_in_student_is_international"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.lag("score", 2, 0).over(w_student_is_international_cou).alias("prev2_score"),
        F.row_number().over(w_all_by_score).alias("rn"),
    )
)

labeled = (
    with_trends
    .filter(F.col("rn").between(2, 9))
    .select(
        "id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "grade_status_prefix",
        "student_name",
        "student_is_international",
        "score_percent_rank",
        "score_share_of_course_id",
        "score_rank_2",
        "next_enrolled_at",
        "rows_in_student_is_international",
        "prev2_score",
        F.when(F.col("grade_status").isin("FAIL", "INCOMPLETE"), "group_a").when(F.col("grade_status") == "PASS", "group_b").otherwise("other").alias("grade_status_group"),
        (F.col("next_enrolled_at") + F.expr("INTERVAL 2 HOURS")).alias("next_enrolled_at_plus_2h"),
    )
)

result = (
    labeled
    .select(
        "id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "grade_status_prefix",
        "student_name",
        "score_percent_rank",
        "score_share_of_course_id",
        "rows_in_student_is_international",
        "prev2_score",
        "next_enrolled_at_plus_2h",
    )
    .orderBy(F.col("grade_status_prefix").desc(), F.col("course_id").desc())
)
