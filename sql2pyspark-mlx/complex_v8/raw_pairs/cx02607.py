from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id")
w_all_by_enrolled_at_month_ful = Window.orderBy("enrolled_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_student_id_by_e = Window.partitionBy("grade_status", "student_id").orderBy(F.col("enrolled_at_plus_2h").desc(), "id")
w_grade_status_student_id_by_e_2 = Window.partitionBy("grade_status", "student_id").orderBy(F.col("enrolled_at_plus_2h").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_month_run = Window.orderBy("enrolled_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_month = Window.orderBy("enrolled_at_month", "id")
w_grade_status_student_id_by_e_3 = Window.partitionBy("grade_status", "student_id").orderBy(F.col("enrolled_at_plus_2h").desc())
w_all_by_enrolled_at_month_tie = Window.orderBy("enrolled_at_month")
w_all_by_enrolled_at_month_mov = Window.orderBy(F.col("enrolled_at_month").desc(), "id").rowsBetween(-1, Window.currentRow)
w_enrolled_at_month_by_enrolle = Window.partitionBy("enrolled_at_month").orderBy(F.col("enrolled_at_plus_2h_seq").desc(), "id")
w_last_enrolled_at_plus_2h_mon = Window.partitionBy(F.date_trunc("month", "last_enrolled_at_plus_2h")).orderBy(F.col("second_enrolled_at_plus_2h").desc_nulls_first())
w_last_enrolled_at_plus_2h_mon_2 = Window.partitionBy(F.date_trunc("month", "last_enrolled_at_plus_2h")).orderBy(F.col("second_enrolled_at_plus_2h").desc_nulls_first(), "id")
w_course_id_enrolled_at_plus_2 = Window.partitionBy("course_id", F.to_date("enrolled_at_plus_2h")).orderBy(F.col("enrolled_at_plus_2h_seq").desc(), "id")

filtered_enrollments = (
    df_enrollments
    .filter((F.col("score") <= 76.8) & (~F.col("grade_status").isin("FAIL", "WITHDRAWN")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.col("enrolled_at") + F.expr("INTERVAL 2 HOURS")).alias("enrolled_at_plus_2h"),
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
    )
)

metrics = (
    filtered_enrollments
    .select(
        "*",
        F.size(F.collect_set("course_id").over(w_student_id)).alias("distinct_course_id_in_student_id"),
        F.last("enrolled_at_plus_2h").over(w_all_by_enrolled_at_month_ful).alias("last_enrolled_at_plus_2h"),
        F.max("enrolled_at_month").over(w_student_id).alias("student_id_max_enrolled_at_month"),
        F.row_number().over(w_grade_status_student_id_by_e).alias("enrolled_at_plus_2h_seq"),
        F.nth_value("enrolled_at_plus_2h", 2).over(w_grade_status_student_id_by_e_2).alias("second_enrolled_at_plus_2h"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
        F.min("score").over(w_all_by_enrolled_at_month_run).alias("min_score_so_far"),
        F.lead("enrolled_at").over(w_all_by_enrolled_at_month).alias("next_enrolled_at"),
        F.first("score", ignorenulls=True).over(w_all_by_enrolled_at_month_run).alias("first_known_score"),
        F.dense_rank().over(w_grade_status_student_id_by_e_3).alias("enrolled_at_plus_2h_dense_rank"),
        F.round(F.avg("score").over(w_student_id), 2).alias("student_id_avg_score"),
        F.size(F.collect_set("term").over(w_student_id)).alias("distinct_term_in_student_id"),
        F.rank().over(w_all_by_enrolled_at_month_tie).alias("enrolled_at_month_rank"),
        F.sum("score").over(w_all_by_enrolled_at_month_mov).alias("score_sum_2"),
        F.round(F.col("enrolled_at_month") / F.nullif(F.sum("enrolled_at_month").over(w_student_id), F.lit(0)), 4).alias("enrolled_at_month_share_of_student_id"),
        F.size(F.collect_set("grade_status").over(w_student_id)).alias("distinct_grade_status_in_student_id"),
    )
)

top_rows = metrics.filter((F.col("enrolled_at_plus_2h_seq") <= 5) & (F.col("enrolled_at_month") > 3))

ranked = (
    top_rows
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "enrolled_at_plus_2h",
        "enrolled_at_month",
        "distinct_course_id_in_student_id",
        "last_enrolled_at_plus_2h",
        "student_id_max_enrolled_at_month",
        "enrolled_at_plus_2h_seq",
        "second_enrolled_at_plus_2h",
        "score_share_of_student_id",
        "min_score_so_far",
        "next_enrolled_at",
        "first_known_score",
        "enrolled_at_plus_2h_dense_rank",
        "student_id_avg_score",
        "distinct_term_in_student_id",
        "enrolled_at_month_rank",
        "enrolled_at_month_share_of_student_id",
        "distinct_grade_status_in_student_id",
        F.lead("distinct_term_in_student_id").over(w_enrolled_at_month_by_enrolle).alias("next_distinct_term_in_student_id"),
        F.rank().over(w_last_enrolled_at_plus_2h_mon).alias("second_enrolled_at_plus_2h_rank"),
        (F.col("distinct_term_in_student_id") - F.lag("distinct_term_in_student_id").over(w_last_enrolled_at_plus_2h_mon_2)).alias("distinct_term_in_student_id_change"),
        F.rank().over(w_course_id_enrolled_at_plus_2).alias("enrolled_at_plus_2h_seq_rank"),
    )
)

stats = (
    ranked
    .groupBy(
        F.date_trunc("month", "last_enrolled_at_plus_2h").alias("last_enrolled_at_plus_2h_month"),
        "enrolled_at_plus_2h_seq_rank",
        "term",
    )
    .agg(
        F.countDistinct("term").alias("distinct_terms"),
        F.max("second_enrolled_at_plus_2h").alias("last_second_enrolled_at_plus_2h"),
        F.count("*").alias("row_count"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
    )
)

result = (
    stats
    .select(
        "last_enrolled_at_plus_2h_month",
        "enrolled_at_plus_2h_seq_rank",
        "term",
        "distinct_terms",
        "last_second_enrolled_at_plus_2h",
        "distinct_course_ids",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
    .orderBy(F.col("distinct_course_ids").desc(), "last_second_enrolled_at_plus_2h")
)
