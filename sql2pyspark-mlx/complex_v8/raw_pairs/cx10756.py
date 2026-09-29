from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_year = Window.partitionBy(F.year("enrolled_at"))
w_enrolled_at_month_by_enrolle = Window.partitionBy(F.date_trunc("month", "enrolled_at")).orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_by_score_adj_runni = Window.partitionBy("course_id").orderBy("score_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score_adj = Window.orderBy("score_adj", "id")
w_student_is_international = Window.partitionBy("student_is_international")
w_course_id_by_score_adj = Window.partitionBy("course_id").orderBy("score_adj", "id")
w_course_id_by_score_adj_full = Window.partitionBy("course_id").orderBy("score_adj", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_is_international_cou = Window.partitionBy("student_is_international", "course_id")
w_course_id_by_score_adj_vs_st = Window.partitionBy("course_id").orderBy(F.col("score_adj_vs_student_is_international_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_by_score_adj_vs_st_2 = Window.partitionBy("course_id").orderBy(F.col("score_adj_vs_student_is_international_avg").desc(), "id")
w_course_id_by_score_adj_vs_st_3 = Window.partitionBy("course_id").orderBy(F.col("score_adj_vs_student_is_international_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_by_max_score_adj_so_far = Window.partitionBy("term").orderBy("max_score_adj_so_far", "id")

base = (
    df_enrollments
    .filter(F.col("term") != "2024T2")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.concat_ws("|", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.round(F.col("score") * 1.1, 2).alias("score_adj"),
    )
)

with_parent = (
    base.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.grade_status_term_label",
        "s.score_adj",
        F.col("st.is_international").alias("student_is_international"),
        "c.course_code",
    )
)

with_trends = (
    with_parent
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "grade_status_term_label",
        "score_adj",
        "student_is_international",
        "course_code",
        F.min("score_adj").over(w_enrolled_at_year).alias("enrolled_at_year_min_score_adj"),
        F.last("enrolled_at").over(w_enrolled_at_month_by_enrolle).alias("last_enrolled_at"),
        F.sum("score_adj").over(w_course_id_by_score_adj_runni).alias("running_score_adj"),
        F.lead("enrolled_at", 3).over(w_all_by_score_adj).alias("next3_enrolled_at"),
        F.round(F.col("score_adj") - F.avg("score_adj").over(w_student_is_international), 2).alias("score_adj_vs_student_is_international_avg"),
        F.lead("enrolled_at", 2).over(w_course_id_by_score_adj).alias("next2_enrolled_at"),
        F.nth_value("score_adj", 2).over(w_course_id_by_score_adj_full).alias("second_score_adj"),
        F.max("score_adj").over(w_course_id_by_score_adj_runni).alias("max_score_adj_so_far"),
        F.row_number().over(w_course_id_by_score_adj).alias("rn"),
        F.size(F.collect_set("term").over(w_student_is_international_cou)).alias("distinct_term_in_student_is_international_course"),
        F.first("score_adj", ignorenulls=True).over(w_course_id_by_score_adj_runni).alias("first_known_score_adj"),
    )
)

ranked = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "grade_status_term_label",
        "score_adj",
        "student_is_international",
        "course_code",
        "running_score_adj",
        "score_adj_vs_student_is_international_avg",
        "next2_enrolled_at",
        "second_score_adj",
        "max_score_adj_so_far",
        "rn",
        F.max("max_score_adj_so_far").over(w_course_id_by_score_adj_vs_st).alias("max_max_score_adj_so_far_so_far"),
        F.dense_rank().over(w_course_id_by_score_adj_vs_st_2).alias("score_adj_vs_student_is_international_avg_dense_"),
        F.nth_value("last_enrolled_at", 2).over(w_course_id_by_score_adj_vs_st_3).alias("second_last_enrolled_at"),
        F.lag("rn").over(w_term_by_max_score_adj_so_far).alias("prev_rn"),
    )
)

result = (
    ranked
    .select("id")
    .distinct()
    .orderBy(F.col("id").asc())
)
