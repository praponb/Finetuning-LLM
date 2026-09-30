from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_major_grade_status_o = Window.partitionBy("student_major", "grade_status_or_unknown").orderBy(F.col("student_enrolled_on").asc_nulls_last(), "id")
w_student_major_grade_status_o_2 = Window.partitionBy("student_major", "grade_status_or_unknown").orderBy(F.col("student_enrolled_on").asc_nulls_last())
w_all_by_enrolled_at_running = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_major_student_id = Window.partitionBy("student_major", "student_id")
w_course_id_student_major_by_s = Window.partitionBy("course_id", "student_major").orderBy("student_enrolled_on")
w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_course_id_student_major_by_s_2 = Window.partitionBy("course_id", "student_major").orderBy("student_enrolled_on", "id")
w_all_by_score_prior_1 = Window.orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(-1, -1)

filtered = (
    df_enrollments
    .filter((F.col("grade_status").isin("INCOMPLETE", "PASS", "FAIL")) & (F.to_date(F.col("enrolled_at")) < F.lit("2025-03-08").cast("date")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

with_parent_student = (
    filtered.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_label",
        "s.grade_status_or_unknown",
        F.col("st.major").alias("student_major"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

ranked = (
    with_parent_student
    .select(
        "*",
        F.lead("score").over(w_student_major_grade_status_o).alias("next_score"),
        F.rank().over(w_student_major_grade_status_o_2).alias("student_enrolled_on_rank"),
        F.sum("score").over(w_all_by_enrolled_at_running).alias("running_score"),
        F.max("score").over(w_student_major_student_id).alias("student_major_student_id_max_score"),
        F.rank().over(w_course_id_student_major_by_s).alias("student_enrolled_on_rank_2"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at), 3).alias("enrolled_at_percent_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_major_student_id), F.lit(0)), 4).alias("score_share_of_student_major_student_id"),
        F.row_number().over(w_student_major_grade_status_o).alias("student_enrolled_on_seq"),
        F.ntile(3).over(w_course_id_student_major_by_s_2).alias("student_enrolled_on_tercile"),
        F.round(F.avg("score").over(w_all_by_score_prior_1), 2).alias("score_avg_1_prior"),
    )
)

stats = (
    ranked
    .groupBy(
        F.to_date("enrolled_at").alias("enrolled_at_day"),
        "student_enrolled_on_tercile",
        "grade_status_or_unknown",
    )
    .agg(
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.max("student_enrolled_on").alias("last_student_enrolled_on"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.count(F.when(F.col("grade_status") == "WITHDRAWN", 1)).alias("n_grade_status_withdrawn"),
        F.round(F.stddev("student_enrolled_on_tercile"), 2).alias("stddev_student_enrolled_on_tercile"),
        F.round(F.sum("student_enrolled_on_rank") / F.nullif(F.sum("enrolled_at_cume_dist"), F.lit(0)), 3).alias("student_enrolled_on_rank_to_enrolled_at_cume_dis"),
    )
)

result = (
    stats
    .select(
        "enrolled_at_day",
        "grade_status_or_unknown",
        "term_list",
        "n_grade_status_withdrawn",
        "student_enrolled_on_rank_to_enrolled_at_cume_dis",
        F.coalesce(F.col("n_grade_status_withdrawn"), F.lit(0)).alias("n_grade_status_withdrawn_filled"),
    )
)
