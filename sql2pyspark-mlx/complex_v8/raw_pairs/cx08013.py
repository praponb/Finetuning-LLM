from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_student_cohort_by = Window.partitionBy("student_id", "student_cohort").orderBy("score", "id")
w_student_cohort_by_student_gp = Window.partitionBy("student_cohort").orderBy("student_gpa")
w_student_id_student_cohort_by_2 = Window.partitionBy("student_id", "student_cohort").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_month_student_is = Window.partitionBy(F.month("enrolled_at"), "student_is_international")
w_student_id_student_cohort_by_3 = Window.partitionBy("student_id", "student_cohort").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_cohort_by_student_gp_2 = Window.partitionBy("student_cohort").orderBy("student_gpa", "id")
w_student_is_international_cou = Window.partitionBy("student_is_international", "course_id").orderBy("student_gpa", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_student_cohort_by_4 = Window.partitionBy("student_id", "student_cohort").orderBy("score")

filtered_enrollments = (
    df_enrollments
    .filter((~F.col("term").isin("2025T2", "2024T1")) & (F.col("student_id") >= 2))
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

combined_details_student = (
    filtered_enrollments.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.major") == "Math"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

with_trends = (
    combined_details_student
    .select(
        "*",
        (F.col("score") - F.lag("score").over(w_student_id_student_cohort_by)).alias("score_change"),
        F.rank().over(w_student_cohort_by_student_gp).alias("student_gpa_rank"),
        F.sum("student_gpa").over(w_student_id_student_cohort_by_2).alias("running_student_gpa"),
        F.round(F.avg("score").over(w_enrolled_at_month_student_is), 2).alias("enrolled_at_month_student_is_international_avg_s"),
        F.size(F.collect_set("course_id").over(w_enrolled_at_month_student_is)).alias("distinct_course_id_in_enrolled_at_month_student_"),
        F.ntile(10).over(w_student_id_student_cohort_by).alias("score_decile"),
        (F.col("student_gpa") - F.lag("student_gpa").over(w_student_id_student_cohort_by)).alias("student_gpa_change"),
        F.nth_value("score", 2).over(w_student_id_student_cohort_by_3).alias("second_score"),
        F.lag("score", 2).over(w_student_cohort_by_student_gp_2).alias("prev2_score"),
        F.round(F.cume_dist().over(w_student_cohort_by_student_gp), 3).alias("student_gpa_cume_dist"),
        F.last("enrolled_at").over(w_student_is_international_cou).alias("last_enrolled_at"),
        F.dense_rank().over(w_student_id_student_cohort_by_4).alias("score_dense_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_month_student_is), F.lit(0)), 4).alias("score_share_of_enrolled_at_month_student_is_inte"),
        F.round(F.percent_rank().over(w_student_id_student_cohort_by), 3).alias("score_percent_rank"),
        F.lead("enrolled_at", 2).over(w_student_cohort_by_student_gp_2).alias("next2_enrolled_at"),
    )
)

labeled = (
    with_trends
    .select(
        "*",
        F.when(F.col("student_cohort").isin("C2024", "C2022"), "group_a").when(F.col("student_cohort") == "C2023", "group_b").otherwise("other").alias("student_cohort_group"),
        F.when(F.col("student_cohort").isin("C2023", "C2022"), "group_a").when(F.col("student_cohort") == "C2021", "group_b").otherwise("other").alias("student_cohort_group_2"),
        F.round(F.col("prev2_score") * 1.07, 2).alias("prev2_score_adj"),
    )
)

result = (
    labeled
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "student_gpa",
        "student_cohort",
        "student_is_international",
        "student_gpa_rank",
        "running_student_gpa",
        "enrolled_at_month_student_is_international_avg_s",
        "student_gpa_change",
        "second_score",
        "prev2_score",
        "student_gpa_cume_dist",
        "last_enrolled_at",
        "score_dense_rank",
        "score_share_of_enrolled_at_month_student_is_inte",
        "score_percent_rank",
        "student_cohort_group_2",
        "prev2_score_adj",
    )
    .orderBy(
        "enrolled_at_month_student_is_international_avg_s",
        F.col("score_dense_rank").desc(),
    )
)
