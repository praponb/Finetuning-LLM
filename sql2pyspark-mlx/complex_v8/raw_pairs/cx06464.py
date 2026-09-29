from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_student_id = Window.partitionBy("grade_status", "student_id")
w_all_by_score_ties = Window.orderBy("score")
w_grade_status_grade_status_le = Window.partitionBy("grade_status", "grade_status_len")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_all_by_score_full = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")
w_all_by_grade_status_grade_st = Window.orderBy("grade_status_grade_status_len_min_score", "id")
w_all_by_grade_status_grade_st_2 = Window.orderBy("grade_status_grade_status_len_min_score", "id").rowsBetween(-3, Window.currentRow)
w_student_id_term_by_grade_sta = Window.partitionBy("student_id", "term").orderBy("grade_status_len_vs_grade_status_student_id_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_term_by_grade_sta_2 = Window.partitionBy("student_id", "term").orderBy("grade_status_len_vs_grade_status_student_id_avg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_terms = Window.partitionBy("distinct_terms")
w_term_distinct_terms_by_stude = Window.partitionBy("term", "distinct_terms").orderBy(F.col("student_id_grade_status_min_score_percent_rank_t").desc(), "term").rowsBetween(-1, Window.currentRow)
w_term_distinct_terms_by_stude_2 = Window.partitionBy("term", "distinct_terms").orderBy(F.col("student_id_grade_status_sum_grade_status_len_low").asc_nulls_last(), "term").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
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
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

combined_details = (
    prepared.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_len",
        "c.course_code",
        F.col("c.level").alias("course_level"),
        "st.student_name",
    )
)

with_windows = (
    combined_details
    .select(
        "*",
        F.round(F.col("grade_status_len") / F.nullif(F.sum("grade_status_len").over(w_grade_status_student_id), F.lit(0)), 4).alias("grade_status_len_share_of_grade_status_student_i"),
        F.round(F.avg("grade_status_len").over(w_grade_status_student_id), 2).alias("grade_status_student_id_avg_grade_status_len"),
        F.round(F.percent_rank().over(w_all_by_score_ties), 3).alias("score_percent_rank"),
        F.min("score").over(w_grade_status_grade_status_le).alias("grade_status_grade_status_len_min_score"),
        F.lag("enrolled_at").over(w_all_by_enrolled_at).alias("prev_enrolled_at"),
        F.round(F.col("grade_status_len") - F.avg("grade_status_len").over(w_grade_status_student_id), 2).alias("grade_status_len_vs_grade_status_student_id_avg"),
        F.nth_value("enrolled_at", 2).over(w_all_by_score_full).alias("second_enrolled_at"),
    )
)

windowed = (
    with_windows
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "course_code",
        "course_level",
        "student_name",
        "grade_status_len_share_of_grade_status_student_i",
        "score_percent_rank",
        "grade_status_len_vs_grade_status_student_id_avg",
        "second_enrolled_at",
        F.min("score_percent_rank").over(w_student_id_grade_status).alias("student_id_grade_status_min_score_percent_rank"),
        F.sum("grade_status_len").over(w_student_id_grade_status).alias("student_id_grade_status_sum_grade_status_len"),
        F.round(F.percent_rank().over(w_all_by_grade_status_grade_st), 3).alias("grade_status_grade_status_len_min_score_percent_"),
        F.max("grade_status_len").over(w_all_by_grade_status_grade_st_2).alias("grade_status_len_max_4"),
        F.max("score_percent_rank").over(w_student_id_grade_status).alias("student_id_grade_status_max_score_percent_rank"),
        F.nth_value("enrolled_at", 2).over(w_student_id_term_by_grade_sta).alias("second_enrolled_at_2"),
        F.first("grade_status_len", ignorenulls=True).over(w_student_id_term_by_grade_sta_2).alias("first_known_grade_status_len"),
    )
)

totals = (
    windowed
    .filter(F.col("student_id") >= 5)
    .groupBy("term")
    .agg(
        F.sum(F.when(F.lower(F.col("student_name")).like("%no%"), F.col("student_id_grade_status_sum_grade_status_len")).otherwise(0)).alias("student_id_grade_status_sum_grade_status_len_low"),
        F.countDistinct("term").alias("distinct_terms"),
        F.round(F.avg("student_id_grade_status_min_score_percent_rank"), 2).alias("avg_student_id_grade_status_min_score_percent_ra"),
        F.round(F.sum("student_id_grade_status_min_score_percent_rank") / F.nullif(F.sum("first_known_grade_status_len"), F.lit(0)), 3).alias("student_id_grade_status_min_score_percent_rank_t"),
    )
)

windowed_2 = (
    totals
    .select(
        "*",
        F.max("distinct_terms").over(w_distinct_terms).alias("distinct_terms_max_distinct_terms"),
        F.round(F.avg("distinct_terms").over(w_term_distinct_terms_by_stude), 2).alias("distinct_terms_avg_2"),
        F.count(F.lit(1)).over(w_term_distinct_terms_by_stude_2).alias("running_count"),
    )
)

result = (
    windowed_2
    .select(
        "term",
        "student_id_grade_status_sum_grade_status_len_low",
        "distinct_terms",
        "student_id_grade_status_min_score_percent_rank_t",
        "distinct_terms_max_distinct_terms",
        "distinct_terms_avg_2",
        "running_count",
    )
    .orderBy("student_id_grade_status_sum_grade_status_len_low")
)
