from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy("score", "id")
w_enrolled_at_to_date_course_i = Window.partitionBy(F.to_date("enrolled_at"), "course_id")
w_student_id = Window.partitionBy("student_id")
w_enrolled_at_month_by_score = Window.partitionBy(F.date_trunc("month", "enrolled_at")).orderBy("score", "id")
w_student_id_by_enrolled_at_pr = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(-2, -1)
w_student_id_by_enrolled_at_mo = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(-2, Window.currentRow)
w_distinct_student_id_in_enrol = Window.partitionBy("distinct_student_id_in_enrolled_at_to_date_cours", "student_id")
w_grade_status_by_distinct_stu = Window.partitionBy("grade_status").orderBy(F.col("distinct_student_id_in_enrolled_at_to_date_cours").asc_nulls_last(), "id")
w_enrolled_at_year_score_tier = Window.partitionBy(F.year("enrolled_at"), "score_tier").orderBy("enrolled_at", "id")

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("student_id") >= 19)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
    )
)

sequenced = (
    cleaned_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.lead("enrolled_at").over(w_all_by_score).alias("next_enrolled_at"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_to_date_course_i), F.lit(0)), 4).alias("score_share_of_enrolled_at_to_date_course_id"),
        F.round(F.col("score") - F.avg("score").over(w_student_id), 2).alias("score_vs_student_id_avg"),
        F.round(F.percent_rank().over(w_enrolled_at_month_by_score), 3).alias("score_percent_rank"),
        F.size(F.collect_set("student_id").over(w_enrolled_at_to_date_course_i)).alias("distinct_student_id_in_enrolled_at_to_date_cours"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
        F.round(F.avg("score").over(w_student_id_by_enrolled_at_pr), 2).alias("score_avg_2_prior"),
        F.sum("score").over(w_student_id).alias("student_id_sum_score"),
        F.max("score").over(w_student_id_by_enrolled_at_mo).alias("score_max_3"),
    )
)

enriched = (
    sequenced
    .filter(F.col("distinct_student_id_in_enrolled_at_to_date_cours") >= 4)
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_vs_student_id_avg",
        "score_percent_rank",
        "distinct_student_id_in_enrolled_at_to_date_cours",
        "score_share_of_student_id",
        "student_id_sum_score",
        "score_max_3",
        F.date_format(F.col("next_enrolled_at"), "MMM yyyy").alias("next_enrolled_at_label"),
        (F.floor(F.col("score_vs_student_id_avg") / 25) * 25).alias("score_vs_student_id_avg_bucket"),
        F.when(F.col("score") >= 77.0, "large").when(F.col("score") >= 16.3, "medium").otherwise("small").alias("score_tier"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.round(F.avg("score").over(w_distinct_student_id_in_enrol), 2).alias("distinct_student_id_in_enrolled_at_to_date_cours_2"),
        F.round(F.percent_rank().over(w_grade_status_by_distinct_stu), 3).alias("distinct_student_id_in_enrolled_at_to_date_cours_3"),
        F.lead("student_id_sum_score").over(w_enrolled_at_year_score_tier).alias("next_student_id_sum_score"),
    )
)

grouped = (
    with_windows
    .filter(F.hour(F.col("enrolled_at")).between(6, 12))
    .groupBy(
        "student_id",
        "grade_status",
        F.to_date("enrolled_at").alias("enrolled_at_day"),
    )
    .agg(
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.percentile_approx("score", 0.5).alias("median_score"),
    )
)

result = (
    grouped
    .select(
        "student_id",
        "enrolled_at_day",
        "grade_status_list",
        "median_score",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at_day")).alias("days_since_enrolled_at_day"),
    )
    .orderBy(F.col("days_since_enrolled_at_day").desc())
)
