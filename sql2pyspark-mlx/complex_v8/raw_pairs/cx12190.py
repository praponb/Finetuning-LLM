from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_grade_status_by_s = Window.partitionBy("student_id", "grade_status").orderBy("score", "id")
w_student_id_course_id_by_scor = Window.partitionBy("student_id", "course_id").orderBy("score", "id")
w_student_id_course_id_by_enro = Window.partitionBy("student_id", "course_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_course_id_by_en = Window.partitionBy("grade_status", "course_id").orderBy("enrolled_at")
w_student_id_course_id_by_scor_2 = Window.partitionBy("student_id", "course_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id")
w_grade_status_course_id_by_en_2 = Window.partitionBy("grade_status", "course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_enrolled_at_to_da = Window.partitionBy("student_id", F.to_date("enrolled_at"))
w_grade_status_by_score_runnin = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_student_id = Window.partitionBy("grade_status", "student_id")
w_grade_status_by_enrolled_at_2 = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_score_seq_by_last_enrolled_a = Window.partitionBy("score_seq").orderBy("last_enrolled_at", "score_seq", "enrolled_at_month")
w_score_seq_last_enrolled_at_t = Window.partitionBy("score_seq", F.to_date("last_enrolled_at"))
w_score_seq_by_last_enrolled_a_2 = Window.partitionBy("score_seq").orderBy("last_enrolled_at", "score_seq", "enrolled_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

sequenced = (
    cleaned
    .select(
        "id",
        "course_id",
        "grade_status",
        "enrolled_at",
        (F.col("score") - F.lag("score").over(w_student_id_grade_status_by_s)).alias("score_change"),
        F.row_number().over(w_student_id_grade_status_by_s).alias("score_seq"),
        (F.col("score") - F.lag("score").over(w_student_id_course_id_by_scor)).alias("score_change_2"),
        F.min("score").over(w_student_id_course_id_by_enro).alias("min_score_so_far"),
        F.nth_value("score", 2).over(w_grade_status_by_enrolled_at).alias("second_score"),
        F.round(F.percent_rank().over(w_grade_status_course_id_by_en), 3).alias("enrolled_at_percent_rank"),
        F.first("score", ignorenulls=True).over(w_student_id_course_id_by_scor_2).alias("first_known_score"),
        F.lag("score", 1, 0).over(w_grade_status_by_score).alias("prev_score"),
        F.last("score", ignorenulls=True).over(w_grade_status_course_id_by_en_2).alias("last_known_score"),
        F.sum("score").over(w_student_id_enrolled_at_to_da).alias("student_id_enrolled_at_to_date_sum_score"),
        F.max("score").over(w_grade_status_by_score_runnin).alias("max_score_so_far"),
        F.max("score").over(w_grade_status_student_id).alias("grade_status_student_id_max_score"),
        F.round(F.percent_rank().over(w_grade_status_by_enrolled_at_2), 3).alias("enrolled_at_percent_rank_2"),
    )
)

scored = (
    sequenced
    .select(
        "id",
        "enrolled_at",
        "score_seq",
        "score_change_2",
        "min_score_so_far",
        "second_score",
        "enrolled_at_percent_rank",
        "prev_score",
        "student_id_enrolled_at_to_date_sum_score",
        "max_score_so_far",
        "grade_status_student_id_max_score",
        "enrolled_at_percent_rank_2",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

stats = (
    scored
    .groupBy(
        "score_seq",
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
    )
    .agg(
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.percentile_approx("enrolled_at_percent_rank", 0.5).alias("median_enrolled_at_percent_rank"),
    )
)

metrics = (
    stats
    .select(
        "*",
        F.lead("score_seq", 2).over(w_score_seq_by_last_enrolled_a).alias("next2_score_seq"),
        F.round(F.avg("score_seq").over(w_score_seq_last_enrolled_at_t), 2).alias("score_seq_last_enrolled_at_to_date_avg_score_seq"),
        F.last("enrolled_at_month", ignorenulls=True).over(w_score_seq_by_last_enrolled_a_2).alias("last_known_enrolled_at_month"),
        F.row_number().over(w_score_seq_by_last_enrolled_a).alias("seq_no"),
    )
)

result = (
    metrics
    .select(
        "score_seq",
        "enrolled_at_month",
        "next2_score_seq",
        "score_seq_last_enrolled_at_to_date_avg_score_seq",
        "last_known_enrolled_at_month",
        "seq_no",
        (F.floor(F.col("next2_score_seq") / 2) * 2).alias("next2_score_seq_bucket"),
    )
    .orderBy(F.col("next2_score_seq").desc(), "enrolled_at_month", "score_seq")
    .limit(5)
)
