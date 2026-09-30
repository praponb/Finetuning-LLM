from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_enrolled_at_ties = Window.partitionBy("term").orderBy("enrolled_at")
w_course_id_term_by_score_ties = Window.partitionBy("course_id", "term").orderBy(F.col("score").desc())
w_student_id = Window.partitionBy("student_id")
w_student_id_grade_status_by_s = Window.partitionBy("student_id", "grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_by_enrolled_at = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_student_id_grade_status_by_s_2 = Window.partitionBy("student_id", "grade_status").orderBy(F.col("score").desc(), "id")
w_course_id_term_by_score_full = Window.partitionBy("course_id", "term").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "grade_status_clean",
        F.dense_rank().over(w_term_by_enrolled_at_ties).alias("enrolled_at_dense_rank"),
        F.round(F.percent_rank().over(w_course_id_term_by_score_ties), 3).alias("score_percent_rank"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.rank().over(w_course_id_term_by_score_ties).alias("score_rank"),
        F.count(F.lit(1)).over(w_student_id_grade_status_by_s).alias("running_count"),
        F.row_number().over(w_term_by_enrolled_at).alias("seq_no"),
        F.size(F.collect_set("term").over(w_student_id)).alias("distinct_term_in_student_id"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_student_id_grade_status_by_s_2))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.first("enrolled_at").over(w_course_id_term_by_score_full).alias("first_enrolled_at"),
    )
)

enriched = (
    sequenced
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "grade_status_clean",
        "score_percent_rank",
        "rows_in_student_id",
        "score_rank",
        "seq_no",
        "distinct_term_in_student_id",
        "minutes_since_prev_enrolled_at",
        "first_enrolled_at",
        F.round(F.col("score_rank") / F.nullif(F.col("score_percent_rank"), F.lit(0)), 3).alias("score_rank_per_score_percent_rank"),
        F.concat_ws(" / ", F.col("grade_status_clean"), F.col("grade_status")).alias("grade_status_clean_grade_status_label"),
        F.concat_ws("-", F.col("grade_status"), F.col("grade_status_clean")).alias("grade_status_grade_status_clean_label"),
    )
)

result = (
    enriched
    .filter(F.upper(F.trim(F.col("grade_status_clean"))).like("I%"))
    .select(
        "id",
        "student_id",
        "grade_status_clean",
        "score_rank",
        "distinct_term_in_student_id",
        "minutes_since_prev_enrolled_at",
        "score_rank_per_score_percent_rank",
        "grade_status_clean_grade_status_label",
        "grade_status_grade_status_clean_label",
        F.lower(F.trim(F.col("grade_status_clean"))).alias("grade_status_clean_clean"),
        F.when(F.col("grade_status").isin("FAIL", "PASS"), "group_a").when(F.col("grade_status") == "WITHDRAWN", "group_b").otherwise("other").alias("grade_status_group"),
    )
    .orderBy(
        F.col("grade_status_clean_grade_status_label").desc(),
        "grade_status_grade_status_clean_label",
        "id",
    )
    .limit(5)
)
