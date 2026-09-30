from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_year_grade_statu = Window.partitionBy("enrolled_at_year", "grade_status")
w_grade_status_term = Window.partitionBy("grade_status", "term")
w_course_id_enrolled_at_year_b = Window.partitionBy("course_id", "enrolled_at_year").orderBy("enrolled_at")
w_course_id_enrolled_at_year_b_2 = Window.partitionBy("course_id", "enrolled_at_year").orderBy("enrolled_at", "id")
w_score_tier_by_enrolled_at_ye = Window.partitionBy("score_tier").orderBy(F.col("enrolled_at_year").asc_nulls_last(), "id")
w_distinct_course_id_in_grade = Window.partitionBy("distinct_course_id_in_grade_status_term").orderBy("enrolled_at_year_grade_status_avg_enrolled_at_ye", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_year = Window.orderBy("enrolled_at_year", "id")
w_student_id = Window.partitionBy("student_id")
w_distinct_course_id_in_grade_2 = Window.partitionBy("distinct_course_id_in_grade_status_term").orderBy("enrolled_at_year_grade_status_avg_enrolled_at_ye", "id")
w_enrolled_at_dense_rank_by_en = Window.partitionBy("enrolled_at_dense_rank").orderBy("enrolled_at", "id")

recent = (
    df_enrollments
    .filter(~F.col("term").isin("2025T2", "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
        F.when(F.col("score") >= 73.0, "HIGH").when(F.col("score") >= 42.4, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

sequenced = (
    recent
    .select(
        "*",
        F.round(F.avg("enrolled_at_year").over(w_enrolled_at_year_grade_statu), 2).alias("enrolled_at_year_grade_status_avg_enrolled_at_ye"),
        F.size(F.collect_set("course_id").over(w_grade_status_term)).alias("distinct_course_id_in_grade_status_term"),
        F.dense_rank().over(w_course_id_enrolled_at_year_b).alias("enrolled_at_dense_rank"),
        F.count(F.lit(1)).over(w_enrolled_at_year_grade_statu).alias("rows_in_enrolled_at_year_grade_status"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_course_id_enrolled_at_year_b_2))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.row_number().over(w_score_tier_by_enrolled_at_ye).alias("enrolled_at_year_seq"),
        (F.col("enrolled_at_year") - F.lag("enrolled_at_year").over(w_course_id_enrolled_at_year_b_2)).alias("enrolled_at_year_change"),
        F.row_number().over(w_course_id_enrolled_at_year_b_2).alias("rn"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "enrolled_at",
        "score_tier",
        "distinct_course_id_in_grade_status_term",
        "rows_in_enrolled_at_year_grade_status",
        "minutes_since_prev_enrolled_at",
        "enrolled_at_year_seq",
        "enrolled_at_year_change",
        "rn",
        F.last("enrolled_at", ignorenulls=True).over(w_distinct_course_id_in_grade).alias("last_known_enrolled_at"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at_year), 3).alias("enrolled_at_year_percent_rank"),
        F.round(F.col("enrolled_at_dense_rank") / F.nullif(F.sum("enrolled_at_dense_rank").over(w_student_id), F.lit(0)), 4).alias("enrolled_at_dense_rank_share_of_student_id"),
        F.lag("enrolled_at_dense_rank").over(w_distinct_course_id_in_grade_2).alias("prev_enrolled_at_dense_rank"),
        F.sum("enrolled_at_year").over(w_student_id).alias("student_id_sum_enrolled_at_year"),
        F.size(F.collect_set("grade_status").over(w_student_id)).alias("distinct_grade_status_in_student_id"),
        F.lead("enrolled_at_year", 3).over(w_enrolled_at_dense_rank_by_en).alias("next3_enrolled_at_year"),
        F.lead("enrolled_at_dense_rank").over(w_all_by_enrolled_at_year).alias("next_enrolled_at_dense_rank"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "term",
        "grade_status",
        "enrolled_at",
        "score_tier",
        "distinct_course_id_in_grade_status_term",
        "rows_in_enrolled_at_year_grade_status",
        "minutes_since_prev_enrolled_at",
        "enrolled_at_year_change",
        "rn",
        "last_known_enrolled_at",
        "prev_enrolled_at_dense_rank",
        "student_id_sum_enrolled_at_year",
        "next3_enrolled_at_year",
        "next_enrolled_at_dense_rank",
    )
    .orderBy(F.col("enrolled_at").desc())
)
