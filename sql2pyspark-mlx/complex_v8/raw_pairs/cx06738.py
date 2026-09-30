from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_grade_status = Window.partitionBy("grade_status")
w_all_by_enrolled_at_moving_2 = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)

prepared = (
    df_enrollments
    .filter(F.col("term").isin("2024T1", "2025T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(" / ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

with_trends = (
    prepared
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "term_grade_status_label",
        F.lead("enrolled_at").over(w_all_by_enrolled_at).alias("next_enrolled_at"),
        F.size(F.collect_set("term").over(w_grade_status)).alias("distinct_term_in_grade_status"),
        F.max("score").over(w_all_by_enrolled_at_moving_2).alias("score_max_3"),
        F.min("score").over(w_grade_status).alias("grade_status_min_score"),
        F.row_number().over(w_all_by_enrolled_at).alias("seq_no"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at), 3).alias("enrolled_at_cume_dist"),
    )
)

result = (
    with_trends
    .groupBy("term")
    .pivot("grade_status", ["WITHDRAWN", "INCOMPLETE", "PASS", "FAIL"])
    .agg(F.count("score_max_3").alias("count_v"), F.sum("score_max_3").alias("sum_v"))
    .orderBy("term")
)
