from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_to_date_by_enrol = Window.partitionBy(F.to_date("enrolled_at")).orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_cohort = Window.partitionBy("student_cohort")
w_student_cohort_by_score_ties = Window.partitionBy("student_cohort").orderBy("score")
w_enrolled_at_to_date_by_enrol_2 = Window.partitionBy(F.to_date("enrolled_at")).orderBy("enrolled_at")
w_term_by_score = Window.partitionBy("term").orderBy("score", "id")

recent_enrollments = (
    df_enrollments
    .filter((F.col("term") == "2025T2") & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.lower(F.trim(F.col("term"))).alias("term_clean"),
        F.when(F.col("grade_status").isin("WITHDRAWN", "PASS"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

enriched_student = (
    recent_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.term_clean",
        "s.grade_status_group",
        F.col("st.cohort").alias("student_cohort"),
    )
)

sequenced = (
    enriched_student
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "term_clean",
        "student_cohort",
        F.first("score", ignorenulls=True).over(w_enrolled_at_to_date_by_enrol).alias("first_known_score"),
        F.count(F.lit(1)).over(w_student_cohort).alias("rows_in_student_cohort"),
        F.sum("score").over(w_enrolled_at_to_date_by_enrol).alias("running_score"),
        F.round(F.cume_dist().over(w_student_cohort_by_score_ties), 3).alias("score_cume_dist"),
        F.rank().over(w_enrolled_at_to_date_by_enrol_2).alias("enrolled_at_rank"),
        F.size(F.collect_set("term").over(w_student_cohort)).alias("distinct_term_in_student_cohort"),
        F.round(F.percent_rank().over(w_enrolled_at_to_date_by_enrol_2), 3).alias("enrolled_at_percent_rank"),
        F.row_number().over(w_term_by_score).alias("score_seq"),
    )
)

enriched = (
    sequenced
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "term_clean",
        "first_known_score",
        "rows_in_student_cohort",
        "running_score",
        "score_cume_dist",
        "distinct_term_in_student_cohort",
        "score_seq",
        (F.col("running_score") - F.col("score_cume_dist")).alias("running_score_minus_score_cume_dist"),
    )
)

result = (
    enriched
    .select(
        "id",
        "student_id",
        "course_id",
        "term_clean",
        "first_known_score",
        "rows_in_student_cohort",
        "running_score",
        "score_cume_dist",
        "distinct_term_in_student_cohort",
        "score_seq",
        "running_score_minus_score_cume_dist",
        F.round(F.col("score_cume_dist") / F.nullif(F.col("running_score"), F.lit(0)), 3).alias("score_cume_dist_per_running_score"),
        F.round(F.col("score_cume_dist") / F.nullif(F.col("first_known_score"), F.lit(0)), 3).alias("score_cume_dist_per_first_known_score"),
    )
    .orderBy(
        F.col("distinct_term_in_student_cohort").asc_nulls_last(),
        F.col("running_score").desc(),
    )
)
