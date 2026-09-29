from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_enrolled_at_mov = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(-3, Window.currentRow)
w_enrolled_at_month_student_id = Window.partitionBy(F.date_trunc("month", "enrolled_at"), "student_id").orderBy("score", "id")
w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_course_id = Window.partitionBy("course_id")
w_enrolled_at_to_date_by_enrol = Window.partitionBy(F.to_date("enrolled_at")).orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_grade_status_by_s = Window.partitionBy("student_id", "grade_status").orderBy("score", "id")
w_course_id_student_id_by_enro = Window.partitionBy("course_id", "student_id").orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_enrolled_at_to_date_by_enrol_2 = Window.partitionBy(F.to_date("enrolled_at")).orderBy("enrolled_at", "id").rowsBetween(-6, Window.currentRow)
w_distinct_student_id_in_grade = Window.partitionBy("distinct_student_id_in_grade_status_course_id")
w_all_by_score_quartile_full = Window.orderBy("score_quartile", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_vs_course_id_av = Window.orderBy(F.col("score_vs_course_id_avg").desc(), "id")
w_course_id_by_score_quintile = Window.partitionBy("course_id").orderBy(F.col("score_quintile").desc_nulls_first(), "id")
w_all_by_score_vs_course_id_av_2 = Window.orderBy(F.col("score_vs_course_id_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score_quartile_ties = Window.orderBy("score_quartile")
w_all_by_score_vs_course_id_av_3 = Window.orderBy(F.col("score_vs_course_id_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-19 07:00:00").cast("timestamp")) | (F.col("grade_status") == "PASS"))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_windows = (
    base
    .select(
        "id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.sum("score").over(w_course_id_by_enrolled_at_mov).alias("score_sum_4"),
        F.row_number().over(w_enrolled_at_month_student_id).alias("seq_no"),
        F.size(F.collect_set("student_id").over(w_grade_status_course_id)).alias("distinct_student_id_in_grade_status_course_id"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status_course_id), 2).alias("score_vs_grade_status_course_id_avg"),
        F.round(F.col("score") - F.avg("score").over(w_course_id), 2).alias("score_vs_course_id_avg"),
        F.min("score").over(w_enrolled_at_to_date_by_enrol).alias("min_score_so_far"),
        F.ntile(4).over(w_student_id_grade_status_by_s).alias("score_quartile"),
        F.lead("score").over(w_course_id_student_id_by_enro).alias("next_score"),
        F.ntile(5).over(w_enrolled_at_month_student_id).alias("score_quintile"),
        F.round(F.avg("score").over(w_enrolled_at_to_date_by_enrol_2), 2).alias("score_avg_7"),
    )
)

with_trends = (
    with_windows
    .select(
        "*",
        F.max("score_avg_7").over(w_distinct_student_id_in_grade).alias("distinct_student_id_in_grade_status_course_id_ma"),
        F.count(F.lit(1)).over(w_distinct_student_id_in_grade).alias("rows_in_distinct_student_id_in_grade_status_cour"),
        F.last("score_quartile").over(w_all_by_score_quartile_full).alias("last_score_quartile"),
        F.row_number().over(w_all_by_score_vs_course_id_av).alias("rn"),
        F.ntile(4).over(w_course_id_by_score_quintile).alias("score_quintile_quartile"),
        F.sum("score_quartile").over(w_all_by_score_vs_course_id_av_2).alias("running_score_quartile"),
        F.round(F.cume_dist().over(w_all_by_score_quartile_ties), 3).alias("score_quartile_cume_dist"),
        F.nth_value("enrolled_at", 2).over(w_all_by_score_vs_course_id_av_3).alias("second_enrolled_at"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "score_sum_4",
        "seq_no",
        "distinct_student_id_in_grade_status_course_id",
        "score_vs_grade_status_course_id_avg",
        "score_vs_course_id_avg",
        "min_score_so_far",
        "score_quartile",
        "next_score",
        "distinct_student_id_in_grade_status_course_id_ma",
        "rows_in_distinct_student_id_in_grade_status_cour",
        "last_score_quartile",
        "rn",
        "score_quartile_cume_dist",
        "second_enrolled_at",
    )
)
