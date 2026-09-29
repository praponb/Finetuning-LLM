from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_grade_status = Window.partitionBy("course_id", "grade_status")
w_course_id_grade_status_by_en = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at", "id").rowsBetween(-2, Window.currentRow)
w_student_id_by_score_adj = Window.partitionBy("student_id").orderBy("score_adj", "id")
w_student_id = Window.partitionBy("student_id")
w_student_id_course_id_by_scor = Window.partitionBy("student_id", "course_id").orderBy("score_adj", "id")
w_course_id_by_score_adj_prior = Window.partitionBy("course_id").orderBy("score_adj", "id").rowsBetween(-3, -1)
w_all_by_score = Window.orderBy(F.col("score").desc_nulls_first(), "id")
w_student_id_course_id_by_scor_2 = Window.partitionBy("student_id", "course_id").orderBy("score_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at")
w_course_id_by_score_adj_movin = Window.partitionBy("course_id").orderBy("score_adj", "id").rowsBetween(-3, Window.currentRow)

base_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
    )
)

with_trends = (
    base_enrollments
    .select(
        "*",
        F.max("score_adj").over(w_course_id_grade_status).alias("course_id_grade_status_max_score_adj"),
        F.sum("score_adj").over(w_course_id_grade_status_by_en).alias("score_adj_sum_3"),
        F.row_number().over(w_student_id_by_score_adj).alias("score_adj_seq"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.lag("enrolled_at").over(w_student_id_course_id_by_scor).alias("prev_enrolled_at"),
        F.round(F.avg("score").over(w_course_id_by_score_adj_prior), 2).alias("score_avg_3_prior"),
        F.rank().over(w_all_by_score).alias("score_rank"),
        F.round(F.col("score_adj") / F.nullif(F.sum("score_adj").over(w_student_id), F.lit(0)), 4).alias("score_adj_share_of_student_id"),
        F.count(F.lit(1)).over(w_student_id_course_id_by_scor_2).alias("running_count"),
        F.sum("score").over(w_course_id_grade_status).alias("course_id_grade_status_sum_score"),
        F.round(F.avg("score_adj").over(w_course_id_grade_status), 2).alias("course_id_grade_status_avg_score_adj"),
        F.round(F.cume_dist().over(w_grade_status_by_enrolled_at), 3).alias("enrolled_at_cume_dist"),
        F.sum("score").over(w_course_id_by_score_adj_movin).alias("score_sum_4"),
        F.round(F.col("score") - F.avg("score").over(w_student_id), 2).alias("score_vs_student_id_avg"),
        F.round(F.col("score_adj") - F.avg("score_adj").over(w_student_id), 2).alias("score_adj_vs_student_id_avg"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "course_id",
        "score_adj",
        "course_id_grade_status_max_score_adj",
        "score_adj_sum_3",
        "score_adj_seq",
        "rows_in_student_id",
        "prev_enrolled_at",
        "score_avg_3_prior",
        "score_adj_share_of_student_id",
        "running_count",
        "enrolled_at_cume_dist",
        "score_sum_4",
        "score_vs_student_id_avg",
        "score_adj_vs_student_id_avg",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
    .orderBy(F.col("score_avg_3_prior").asc(), "id")
    .limit(10)
)
