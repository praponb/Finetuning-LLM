from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term")
w_student_id_term_by_score_run = Window.partitionBy("student_id", "term").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(-6, Window.currentRow)
w_course_id = Window.partitionBy("course_id")
w_grade_status_enrolled_at_to = Window.partitionBy("grade_status", F.to_date("enrolled_at")).orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_all_by_enrolled_at_running = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

joined_student = (
    base_enrollments.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.lower(F.col("st.student_name")).like("%sc%")), "inner")
    .select("s.*", F.col("st.cohort").alias("student_cohort"))
)

windowed = (
    joined_student
    .select(
        "*",
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.sum("score").over(w_student_id_term_by_score_run).alias("running_score"),
        F.round(F.avg("score").over(w_grade_status_by_enrolled_at), 2).alias("score_avg_7"),
        F.round(F.avg("score").over(w_term), 2).alias("term_avg_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_term), F.lit(0)), 4).alias("score_share_of_term"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.lead("score", 1, 0).over(w_grade_status_enrolled_at_to).alias("next_score"),
        F.sum("score").over(w_term).alias("term_sum_score"),
        F.dense_rank().over(w_grade_status_enrolled_at_to).alias("enrolled_at_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_percent_rank"),
        F.first("enrolled_at", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("first_known_enrolled_at"),
        F.size(F.collect_set("grade_status").over(w_course_id)).alias("distinct_grade_status_in_course_id"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_cume_dist"),
        F.ntile(3).over(w_grade_status_enrolled_at_to).alias("enrolled_at_tercile"),
    )
)

top_per_group = (
    windowed
    .filter(F.col("enrolled_at_dense_rank") == 1)
    .drop("enrolled_at_dense_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "rows_in_term",
        "score_avg_7",
        "term_avg_score",
        "score_share_of_term",
        "rows_in_course_id",
        "next_score",
        "term_sum_score",
        "enrolled_at_percent_rank",
        "first_known_enrolled_at",
        "distinct_grade_status_in_course_id",
        "enrolled_at_cume_dist",
        "enrolled_at_tercile",
    )
)
