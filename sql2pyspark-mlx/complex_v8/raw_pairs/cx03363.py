from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_distinct_grade = Window.partitionBy("grade_status", "distinct_grade_status")
w_all_by_n_grade_status_is_not = Window.orderBy("n_grade_status_is_not_null", "grade_status", "enrolled_at_ym").rowsBetween(-6, -1)
w_all_by_n_grade_status_is_not_2 = Window.orderBy("n_grade_status_is_not_null", "grade_status", "enrolled_at_ym").rowsBetween(-3, -1)
w_all_by_n_grade_status_is_not_3 = Window.orderBy("n_grade_status_is_not_null")

base = (
    df_enrollments
    .filter(F.col("course_id") <= 5)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

stats = (
    base
    .groupBy(
        "grade_status",
        F.date_format("enrolled_at", "yyyy-MM").alias("enrolled_at_ym"),
    )
    .agg(
        F.round(F.sum("score"), 2).alias("total_score"),
        F.count("*").alias("row_count"),
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.count(F.when(F.col("grade_status").isNotNull(), 1)).alias("n_grade_status_is_not_null"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
        F.sum("score").alias("sum_score_check"),
    )
    .filter(F.col("sum_score_check") > 34.2)
    .drop("sum_score_check")
)

windowed = (
    stats
    .select(
        "*",
        F.max("distinct_course_ids").over(w_grade_status_distinct_grade).alias("grade_status_distinct_grade_status_max_distinct_"),
        F.sum("distinct_grade_status").over(w_all_by_n_grade_status_is_not).alias("distinct_grade_status_sum_6_prior"),
        F.sum("total_score").over(w_all_by_n_grade_status_is_not_2).alias("total_score_sum_3_prior"),
        F.rank().over(w_all_by_n_grade_status_is_not_3).alias("n_grade_status_is_not_null_rank"),
    )
)

result = (
    windowed
    .select(
        "grade_status",
        "total_score",
        "distinct_course_ids",
        "n_grade_status_is_not_null",
        "distinct_grade_status",
        "grade_status_distinct_grade_status_max_distinct_",
        "distinct_grade_status_sum_6_prior",
        "total_score_sum_3_prior",
        "n_grade_status_is_not_null_rank",
    )
    .orderBy(F.col("distinct_grade_status_sum_6_prior").desc())
)
