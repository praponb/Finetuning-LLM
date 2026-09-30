from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_grade_status_l = Window.partitionBy("student_id").orderBy(F.col("grade_status_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_by_grade_status_l_2 = Window.partitionBy("student_id").orderBy(F.col("grade_status_len").desc(), "id")
w_grade_status_by_score_full = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_by_score_runnin = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status = Window.partitionBy("grade_status")
w_all_by_grade_status_len = Window.orderBy(F.col("grade_status_len").desc_nulls_first(), "id")
w_student_id_by_grade_status_l_3 = Window.partitionBy("student_id").orderBy(F.col("grade_status_len").desc())
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id")
w_course_id = Window.partitionBy("course_id")
w_all_by_grade_status_len_movi = Window.orderBy(F.col("grade_status_len").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_grade_status_len_prio = Window.orderBy(F.col("grade_status_len").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_grade_status_len_rank_2_by_l = Window.partitionBy("grade_status_len_rank_2").orderBy(F.col("last_known_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_len_rank_distin = Window.partitionBy("grade_status_len_rank", "distinct_term_in_course_id")

recent = (
    df_enrollments
    .filter((F.col("term") == "2025T1") & (F.col("course_id") <= 2))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

enriched = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
    )
)

sequenced = (
    enriched
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status_len",
        "grade_status_slug",
        "course_credits",
        "course_code",
        F.last("score", ignorenulls=True).over(w_student_id_by_grade_status_l).alias("last_known_score"),
        F.rank().over(w_student_id_by_grade_status_l_2).alias("grade_status_len_rank"),
        F.last("grade_status_len").over(w_grade_status_by_score_full).alias("last_grade_status_len"),
        F.last("score", ignorenulls=True).over(w_grade_status_by_score_runnin).alias("last_known_score_2"),
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
        F.rank().over(w_all_by_grade_status_len).alias("grade_status_len_rank_2"),
        F.rank().over(w_student_id_by_grade_status_l_3).alias("grade_status_len_rank_3"),
        F.row_number().over(w_grade_status_by_score).alias("rn"),
        F.size(F.collect_set("term").over(w_course_id)).alias("distinct_term_in_course_id"),
        F.round(F.avg("course_credits").over(w_all_by_grade_status_len_movi), 2).alias("course_credits_avg_7"),
        F.sum("score").over(w_all_by_grade_status_len_prio).alias("score_sum_3_prior"),
        F.ntile(4).over(w_grade_status_by_score).alias("score_quartile"),
        F.lead("score", 2).over(w_all_by_grade_status_len).alias("next2_score"),
    )
)

with_windows = (
    sequenced
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status_len",
        "grade_status_slug",
        "course_credits",
        "course_code",
        "last_known_score",
        "last_grade_status_len",
        "last_known_score_2",
        "rows_in_grade_status",
        "grade_status_len_rank_2",
        "grade_status_len_rank_3",
        "rn",
        "distinct_term_in_course_id",
        "score_quartile",
        "next2_score",
        F.first("course_credits", ignorenulls=True).over(w_grade_status_len_rank_2_by_l).alias("first_known_course_credits"),
        F.count(F.lit(1)).over(w_grade_status_len_rank_distin).alias("rows_in_grade_status_len_rank_distinct_term_in_c"),
        F.round(F.col("rows_in_grade_status") - F.avg("rows_in_grade_status").over(w_grade_status_len_rank_distin), 2).alias("rows_in_grade_status_vs_grade_status_len_rank_di"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status_slug",
        "course_credits",
        "course_code",
        "last_known_score",
        "last_grade_status_len",
        "last_known_score_2",
        "rows_in_grade_status",
        "grade_status_len_rank_3",
        "rn",
        "distinct_term_in_course_id",
        "next2_score",
        "first_known_course_credits",
        "rows_in_grade_status_len_rank_distinct_term_in_c",
        "rows_in_grade_status_vs_grade_status_len_rank_di",
        F.abs(F.col("grade_status_len") - 9).alias("grade_status_len_distance"),
        F.abs(F.col("grade_status_len") - 7).alias("grade_status_len_distance_2"),
    )
    .orderBy(F.col("first_known_course_credits").asc(), "id")
    .limit(10)
)
