from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_term = Window.partitionBy("student_id", "term")
w_enrolled_at_month_student_id = Window.partitionBy(F.month("enrolled_at"), "student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_course_id_by_student_gpa_ful = Window.partitionBy("course_id").orderBy("student_gpa", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_course_id = Window.partitionBy("term", "course_id")
w_enrolled_at_month_student_id_2 = Window.partitionBy(F.month("enrolled_at"), "student_id").orderBy(F.col("enrolled_at").desc())
w_term_by_student_gpa = Window.partitionBy("term").orderBy(F.col("student_gpa").desc(), "id")
w_student_id_term_by_student_g = Window.partitionBy("student_id", "term").orderBy(F.col("student_gpa").asc_nulls_last(), "id")
w_course_id_by_student_gpa = Window.partitionBy("course_id").orderBy("student_gpa", "id")
w_course_id_by_student_gpa_tie = Window.partitionBy("course_id").orderBy("student_gpa")
w_student_id_term_by_student_g_2 = Window.partitionBy("student_id", "term").orderBy(F.col("student_gpa").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term = Window.partitionBy("term")
w_student_gpa_quintile_student = Window.partitionBy("student_gpa_quintile", "student_id")
w_all_by_student_gpa_vs_term_c = Window.orderBy(F.col("student_gpa_vs_term_course_id_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_rank_runn = Window.orderBy(F.col("enrolled_at_rank").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_rank_prio = Window.orderBy(F.col("enrolled_at_rank").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_student_gpa_quintile_by_days = Window.partitionBy("student_gpa_quintile").orderBy("days_since_enrolled_at")

prepared = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

combined_details = (
    prepared.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.grade_status_term_label",
        F.col("st.gpa").alias("student_gpa"),
    )
)

metrics = (
    combined_details
    .select(
        "*",
        F.count(F.lit(1)).over(w_student_id_term).alias("rows_in_student_id_term"),
        (F.col("student_gpa") - F.lag("student_gpa").over(w_enrolled_at_month_student_id)).alias("student_gpa_change"),
        F.last("student_gpa").over(w_course_id_by_student_gpa_ful).alias("last_student_gpa"),
        F.round(F.col("student_gpa") - F.avg("student_gpa").over(w_term_course_id), 2).alias("student_gpa_vs_term_course_id_avg"),
        F.rank().over(w_enrolled_at_month_student_id_2).alias("enrolled_at_rank"),
        F.max("student_gpa").over(w_student_id_term).alias("student_id_term_max_student_gpa"),
        F.round(F.col("student_gpa") / F.nullif(F.sum("student_gpa").over(w_student_id_term), F.lit(0)), 4).alias("student_gpa_share_of_student_id_term"),
        F.dense_rank().over(w_term_by_student_gpa).alias("student_gpa_dense_rank"),
        F.row_number().over(w_student_id_term_by_student_g).alias("seq_no"),
        F.ntile(5).over(w_course_id_by_student_gpa).alias("student_gpa_quintile"),
        F.round(F.cume_dist().over(w_course_id_by_student_gpa_tie), 3).alias("student_gpa_cume_dist"),
        F.first("enrolled_at", ignorenulls=True).over(w_student_id_term_by_student_g_2).alias("first_known_enrolled_at"),
        F.round(F.col("student_gpa") - F.avg("student_gpa").over(w_term), 2).alias("student_gpa_vs_term_avg"),
    )
)

labeled = (
    metrics
    .filter(F.col("student_gpa") < 3.3)
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at",
        "student_gpa",
        "rows_in_student_id_term",
        "student_gpa_change",
        "student_gpa_vs_term_course_id_avg",
        "enrolled_at_rank",
        "student_gpa_share_of_student_id_term",
        "student_gpa_dense_rank",
        "student_gpa_quintile",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

with_trends = (
    labeled
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at",
        "student_gpa",
        "student_gpa_change",
        "student_gpa_vs_term_course_id_avg",
        "enrolled_at_rank",
        "student_gpa_dense_rank",
        "student_gpa_quintile",
        "days_since_enrolled_at",
        F.round(F.col("rows_in_student_id_term") / F.nullif(F.sum("rows_in_student_id_term").over(w_student_gpa_quintile_student), F.lit(0)), 4).alias("rows_in_student_id_term_share_of_student_gpa_qui"),
        F.round(F.col("student_gpa") - F.avg("student_gpa").over(w_student_gpa_quintile_student), 2).alias("student_gpa_vs_student_gpa_quintile_student_id_a"),
        F.last("student_gpa_dense_rank", ignorenulls=True).over(w_all_by_student_gpa_vs_term_c).alias("last_known_student_gpa_dense_rank"),
        F.count(F.lit(1)).over(w_student_gpa_quintile_student).alias("rows_in_student_gpa_quintile_student_id"),
        F.sum("student_gpa_change").over(w_all_by_enrolled_at_rank_runn).alias("running_student_gpa_change"),
        F.round(F.avg("enrolled_at_rank").over(w_all_by_enrolled_at_rank_prio), 2).alias("enrolled_at_rank_avg_1_prior"),
        F.max("student_gpa_quintile").over(w_all_by_student_gpa_vs_term_c).alias("max_student_gpa_quintile_so_far"),
        F.round(F.cume_dist().over(w_student_gpa_quintile_by_days), 3).alias("days_since_enrolled_at_cume_dist"),
    )
)

result = (
    with_trends
    .filter(F.col("student_gpa_change") > -0.8)
    .select(
        "id",
        "term",
        "enrolled_at",
        "student_gpa",
        "student_gpa_change",
        "student_gpa_vs_term_course_id_avg",
        "enrolled_at_rank",
        "student_gpa_dense_rank",
        "student_gpa_quintile",
        "days_since_enrolled_at",
        "student_gpa_vs_student_gpa_quintile_student_id_a",
        "last_known_student_gpa_dense_rank",
        "rows_in_student_gpa_quintile_student_id",
        "running_student_gpa_change",
        "days_since_enrolled_at_cume_dist",
    )
    .orderBy(F.col("student_gpa_change").desc_nulls_first())
)
