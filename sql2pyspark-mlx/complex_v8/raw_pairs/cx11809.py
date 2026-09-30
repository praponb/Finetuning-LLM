from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id")
w_all_by_course_credits_prior = Window.orderBy("course_credits", "id").rowsBetween(-2, -1)
w_student_cohort_by_course_cre = Window.partitionBy("student_cohort").orderBy("course_credits", "id")
w_all_by_course_credits = Window.orderBy("course_credits", "id")
w_student_cohort_course_credit = Window.partitionBy("student_cohort", "course_credits_decile").orderBy(F.col("course_credits_decile").desc())
w_all_by_distinct_course_level = Window.orderBy(F.col("distinct_course_level_in_course_id").desc(), "id")
w_all_by_distinct_course_level_2 = Window.orderBy(F.col("distinct_course_level_in_course_id").desc())
w_student_cohort_course_credit_2 = Window.partitionBy("student_cohort", "course_credits_decile").orderBy(F.col("course_credits_decile").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_enrollments
    .filter(F.col("course_id") <= 9)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

joined_course = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
        F.col("st.major").alias("student_major"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

with_windows = (
    joined_course
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "course_credits",
        "course_level",
        "student_major",
        "student_cohort",
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.round(F.avg("course_credits").over(w_all_by_course_credits_prior), 2).alias("course_credits_avg_2_prior"),
        F.size(F.collect_set("course_level").over(w_course_id)).alias("distinct_course_level_in_course_id"),
        F.ntile(10).over(w_student_cohort_by_course_cre).alias("course_credits_decile"),
        F.lead("course_credits", 1, 0).over(w_all_by_course_credits).alias("next_course_credits"),
    )
)

windowed = (
    with_windows
    .select(
        "*",
        F.round(F.cume_dist().over(w_student_cohort_course_credit), 3).alias("course_credits_decile_cume_dist"),
        F.dense_rank().over(w_all_by_distinct_course_level).alias("distinct_course_level_in_course_id_dense_rank"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id_2"),
        F.size(F.collect_set("student_id").over(w_course_id)).alias("distinct_student_id_in_course_id"),
        F.max("course_credits_avg_2_prior").over(w_course_id).alias("course_id_max_course_credits_avg_2_prior"),
        F.dense_rank().over(w_all_by_distinct_course_level_2).alias("distinct_course_level_in_course_id_dense_rank_2"),
        F.last("course_credits_decile", ignorenulls=True).over(w_student_cohort_course_credit_2).alias("last_known_course_credits_decile"),
    )
)

result = (
    windowed
    .filter(F.col("distinct_course_level_in_course_id") >= 4)
    .select(
        "id",
        "course_id",
        "term",
        "course_credits",
        "course_level",
        "rows_in_course_id",
        "course_credits_avg_2_prior",
        "distinct_course_level_in_course_id",
        "course_credits_decile",
        "next_course_credits",
        "course_credits_decile_cume_dist",
        "distinct_course_level_in_course_id_dense_rank",
        "rows_in_course_id_2",
        "course_id_max_course_credits_avg_2_prior",
        "last_known_course_credits_decile",
        F.least(F.col("course_credits_decile"), F.col("rows_in_course_id_2")).alias("least_course_credits_decile_rows_in_course_id_2"),
    )
    .orderBy(F.col("rows_in_course_id_2").desc())
)
