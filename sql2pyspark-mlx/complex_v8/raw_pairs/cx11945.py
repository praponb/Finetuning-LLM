from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_year = Window.partitionBy(F.year("enrolled_at"))
w_grade_status_student_id = Window.partitionBy("grade_status", "student_id")
w_enrolled_at_year_student_id = Window.partitionBy(F.year("enrolled_at"), "student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_course_id_by_enrolled_at_run = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_by_score_moving = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(-1, Window.currentRow)
w_first_known_enrolled_at_year = Window.partitionBy(F.year("first_known_enrolled_at"), "course_id")
w_all_by_score_avg_2 = Window.orderBy("score_avg_2", "id")

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("course_id") <= 7)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
    )
)

windowed = (
    cleaned_enrollments
    .select(
        "*",
        F.max("score").over(w_enrolled_at_year).alias("enrolled_at_year_max_score"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status_student_id), 2).alias("score_vs_grade_status_student_id_avg"),
        F.row_number().over(w_enrolled_at_year_student_id).alias("seq_no"),
        F.count(F.lit(1)).over(w_enrolled_at_year).alias("rows_in_enrolled_at_year"),
        F.first("enrolled_at", ignorenulls=True).over(w_course_id_by_enrolled_at_run).alias("first_known_enrolled_at"),
        F.max("score").over(w_course_id_by_enrolled_at_run).alias("max_score_so_far"),
        F.round(F.avg("score").over(w_grade_status_by_score_moving), 2).alias("score_avg_2"),
    )
)

first_rows = windowed.filter((F.col("seq_no") <= 5) & (F.col("score_avg_2") >= 15.0))

metrics = (
    first_rows
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "enrolled_at_year_max_score",
        "seq_no",
        "rows_in_enrolled_at_year",
        "first_known_enrolled_at",
        "max_score_so_far",
        "score_avg_2",
        F.round(F.col("seq_no") - F.avg("seq_no").over(w_first_known_enrolled_at_year), 2).alias("seq_no_vs_first_known_enrolled_at_year_course_id"),
        F.round(F.col("rows_in_enrolled_at_year") / F.nullif(F.sum("rows_in_enrolled_at_year").over(w_first_known_enrolled_at_year), F.lit(0)), 4).alias("rows_in_enrolled_at_year_share_of_first_known_en"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_score_avg_2))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        (F.col("max_score_so_far") - F.lag("max_score_so_far").over(w_all_by_score_avg_2)).alias("max_score_so_far_change"),
    )
)

result = (
    metrics
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at",
        "enrolled_at_year_max_score",
        "seq_no",
        "rows_in_enrolled_at_year",
        "first_known_enrolled_at",
        "max_score_so_far",
        "score_avg_2",
        "seq_no_vs_first_known_enrolled_at_year_course_id",
        "minutes_since_prev_enrolled_at",
        "max_score_so_far_change",
        F.round((F.unix_timestamp(F.col("enrolled_at")) - F.unix_timestamp(F.col("first_known_enrolled_at"))) / 60, 1).alias("minutes_first_known_enrolled_at_to_enrolled_at"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("rows_in_enrolled_at_year").cast("string")).alias("grade_status_ref"),
    )
    .orderBy(
        F.col("score_avg_2").desc(),
        F.col("seq_no_vs_first_known_enrolled_at_year_course_id").desc(),
    )
)
