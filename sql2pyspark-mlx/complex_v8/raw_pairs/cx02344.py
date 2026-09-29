from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_or_unknown_grad = Window.partitionBy("grade_status_or_unknown", "grade_status").orderBy(F.col("score").desc())
w_grade_status_course_id_by_sc = Window.partitionBy("grade_status", "course_id").orderBy("score", "id")
w_grade_status_or_unknown = Window.partitionBy("grade_status_or_unknown")
w_grade_status_or_unknown_cour = Window.partitionBy("grade_status_or_unknown", "course_level").orderBy("score")
w_grade_status_or_unknown_cour_2 = Window.partitionBy("grade_status_or_unknown", "course_level").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_grade_status = Window.partitionBy("course_id", "grade_status")
w_grade_status_or_unknown_grad_2 = Window.partitionBy("grade_status_or_unknown", "grade_status").orderBy(F.col("score").desc(), "id")
w_course_level_grade_status_or = Window.partitionBy("course_level", "grade_status_or_unknown")
w_grade_status_course_id_by_sc_2 = Window.partitionBy("grade_status", "course_id").orderBy("score")
w_grade_status_or_unknown_cour_3 = Window.partitionBy("grade_status_or_unknown", "course_level").orderBy("score", "id").rowsBetween(-1, Window.currentRow)
w_student_id_grade_status_by_s = Window.partitionBy("student_id", "grade_status").orderBy("score")
w_all_by_score = Window.orderBy("score", "id")
w_grade_status = Window.partitionBy("grade_status")
w_grade_status_by_score_avg_2 = Window.partitionBy("grade_status").orderBy(F.col("score_avg_2").desc(), "id")

recent = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

joined = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.grade_status_or_unknown",
        F.col("c.level").alias("course_level"),
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
    )
)

sequenced = (
    joined
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "course_level",
        "course_code",
        "course_dept",
        F.round(F.cume_dist().over(w_grade_status_or_unknown_grad), 3).alias("score_cume_dist"),
        F.lead("score", 1, 0).over(w_grade_status_course_id_by_sc).alias("next_score"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status_or_unknown), 2).alias("score_vs_grade_status_or_unknown_avg"),
        F.round(F.cume_dist().over(w_grade_status_or_unknown_cour), 3).alias("score_cume_dist_2"),
        F.nth_value("score", 2).over(w_grade_status_or_unknown_cour_2).alias("second_score"),
        F.count(F.lit(1)).over(w_course_id_grade_status).alias("rows_in_course_id_grade_status"),
        F.row_number().over(w_grade_status_or_unknown_grad_2).alias("seq_no"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_level_grade_status_or), F.lit(0)), 4).alias("score_share_of_course_level_grade_status_or_unkn"),
        F.round(F.cume_dist().over(w_grade_status_course_id_by_sc_2), 3).alias("score_cume_dist_3"),
        F.round(F.avg("score").over(w_grade_status_or_unknown_cour_3), 2).alias("score_avg_2"),
        F.size(F.collect_set("grade_status_or_unknown").over(w_course_id_grade_status)).alias("distinct_grade_status_or_unknown_in_course_id_gr"),
        F.size(F.collect_set("course_level").over(w_course_id_grade_status)).alias("distinct_course_level_in_course_id_grade_status"),
        F.round(F.percent_rank().over(w_grade_status_course_id_by_sc_2), 3).alias("score_percent_rank"),
        (F.col("score") - F.lag("score").over(w_grade_status_course_id_by_sc)).alias("score_change"),
        F.rank().over(w_student_id_grade_status_by_s).alias("score_rank"),
        F.round(F.cume_dist().over(w_all_by_score), 3).alias("score_cume_dist_4"),
    )
)

leaders = sequenced.filter(F.col("seq_no") == 1)

sequenced_2 = (
    leaders
    .select(
        "id",
        "student_id",
        "course_id",
        "course_code",
        "course_dept",
        "score_vs_grade_status_or_unknown_avg",
        "second_score",
        "rows_in_course_id_grade_status",
        "score_share_of_course_level_grade_status_or_unkn",
        "score_cume_dist_3",
        "score_avg_2",
        "distinct_course_level_in_course_id_grade_status",
        "score_percent_rank",
        "score_change",
        "score_rank",
        "score_cume_dist_4",
        F.min("score").over(w_grade_status).alias("grade_status_min_score"),
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
        F.ntile(10).over(w_grade_status_by_score_avg_2).alias("score_avg_2_decile"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "course_id",
        "course_code",
        "course_dept",
        "second_score",
        "rows_in_course_id_grade_status",
        "score_share_of_course_level_grade_status_or_unkn",
        "score_cume_dist_3",
        "score_avg_2",
        "distinct_course_level_in_course_id_grade_status",
        "score_percent_rank",
        "score_change",
        "score_cume_dist_4",
        "grade_status_min_score",
        "rows_in_grade_status",
        "score_avg_2_decile",
        F.abs(F.col("score_cume_dist_3") - 0.6).alias("score_cume_dist_3_distance"),
    )
    .orderBy("score_cume_dist_4")
)
