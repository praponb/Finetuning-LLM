from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_filled = Window.orderBy("score_filled", "id")
w_grade_status_course_id_by_en = Window.partitionBy("grade_status", "course_id").orderBy(F.col("enrolled_at").desc())
w_enrolled_at_year = Window.partitionBy(F.year("enrolled_at"))
w_grade_status_course_id_by_en_2 = Window.partitionBy("grade_status", "course_id").orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_score_filled_running = Window.orderBy("score_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_course_id_by_en_3 = Window.partitionBy("grade_status", "course_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_enrollments = (
    df_enrollments
    .filter((F.hour(F.col("enrolled_at")).between(6, 12)) | (F.col("score") < 78.8))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

windowed = (
    prepared_enrollments
    .select(
        "*",
        F.lag("score", 3).over(w_all_by_score_filled).alias("prev3_score"),
        F.round(F.percent_rank().over(w_grade_status_course_id_by_en), 3).alias("enrolled_at_percent_rank"),
        F.round(F.col("score") - F.avg("score").over(w_enrolled_at_year), 2).alias("score_vs_enrolled_at_year_avg"),
        F.lag("score_filled").over(w_all_by_score_filled).alias("prev_score_filled"),
        F.lead("enrolled_at").over(w_grade_status_course_id_by_en_2).alias("next_enrolled_at"),
        F.size(F.collect_set("student_id").over(w_enrolled_at_year)).alias("distinct_student_id_in_enrolled_at_year"),
        F.last("enrolled_at", ignorenulls=True).over(w_all_by_score_filled_running).alias("last_known_enrolled_at"),
        F.last("score").over(w_grade_status_course_id_by_en_3).alias("last_score"),
    )
)

tagged = (
    windowed
    .select(
        "*",
        F.abs(F.col("last_score") - 38.3).alias("last_score_distance"),
        (F.floor(F.col("prev3_score") / 25) * 25).alias("prev3_score_bucket"),
    )
)

result = (
    tagged
    .filter(F.col("score_filled") <= 85.0)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_filled",
        "prev3_score",
        "enrolled_at_percent_rank",
        "score_vs_enrolled_at_year_avg",
        "prev_score_filled",
        "next_enrolled_at",
        "distinct_student_id_in_enrolled_at_year",
        "last_score",
        "last_score_distance",
    )
    .orderBy("enrolled_at_percent_rank")
)
