from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_enrolled_at_to = Window.partitionBy("grade_status", F.to_date("enrolled_at")).orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_score_ties = Window.orderBy(F.col("score").desc())
w_grade_status_term = Window.partitionBy("grade_status", "term")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_student_id_enrolled_at_month = Window.partitionBy("student_id", F.date_trunc("month", "enrolled_at"))
w_course_id = Window.partitionBy("course_id")
w_student_id_grade_status_by_s = Window.partitionBy("student_id", "grade_status").orderBy(F.col("score").desc(), "id")
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_cnt_by_median_score_rank = Window.partitionBy("cnt").orderBy(F.col("median_score_rank").desc(), "rn", "term")
w_cnt_by_median_score_rank_ful = Window.partitionBy("cnt").orderBy(F.col("median_score_rank").desc(), "rn", "term").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_enrollments
    .filter((F.col("grade_status") != "INCOMPLETE") & (F.col("student_id") >= 13))
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

sequenced = (
    prepared
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.round(F.avg("score").over(w_grade_status_enrolled_at_to), 2).alias("score_avg_2"),
        F.dense_rank().over(w_all_by_score_ties).alias("score_dense_rank"),
        F.round(F.avg("score").over(w_grade_status_term), 2).alias("grade_status_term_avg_score"),
        F.row_number().over(w_all_by_enrolled_at).alias("rn"),
        F.round(F.avg("score").over(w_student_id_enrolled_at_month), 2).alias("student_id_enrolled_at_month_avg_score"),
        F.sum("score").over(w_course_id).alias("course_id_sum_score"),
        F.lag("score").over(w_all_by_enrolled_at).alias("prev_score"),
        F.rank().over(w_student_id_grade_status_by_s).alias("score_rank"),
        F.sum("score").over(w_student_id_enrolled_at_month).alias("student_id_enrolled_at_month_sum_score"),
        F.sum("score").over(w_grade_status_term).alias("grade_status_term_sum_score"),
        F.max("score").over(w_course_id).alias("course_id_max_score"),
        F.count(F.lit(1)).over(w_student_id_enrolled_at_month).alias("rows_in_student_id_enrolled_at_month"),
        F.first("enrolled_at").over(w_all_by_enrolled_at_full).alias("first_enrolled_at"),
    )
)

stats = (
    sequenced
    .groupBy("rn", "term")
    .agg(
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.percentile_approx("score_rank", 0.5).alias("median_score_rank"),
        F.min("rows_in_student_id_enrolled_at_month").alias("min_rows_in_student_id_enrolled_at_month"),
        F.round(F.avg("grade_status_term_sum_score"), 2).alias("avg_grade_status_term_sum_score"),
    )
)

metrics = (
    stats
    .withColumn("rn_2", F.row_number().over(w_cnt_by_median_score_rank))
    .withColumn("first_cnt", F.first("cnt").over(w_cnt_by_median_score_rank_ful))
)

result = (
    metrics
    .select("term", "cnt")
    .distinct()
    .orderBy("term")
)
