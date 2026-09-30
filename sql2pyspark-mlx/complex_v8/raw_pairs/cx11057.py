from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_month = Window.partitionBy(F.month("enrolled_at"))
w_student_id_course_level_by_s = Window.partitionBy("student_id", "course_level").orderBy(F.col("score").desc())
w_student_id_course_level_by_s_2 = Window.partitionBy("student_id", "course_level").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_credits_enrolled_at_y = Window.partitionBy("course_credits", F.year("enrolled_at")).orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_enrolled_at_year = Window.partitionBy("student_id", F.year("enrolled_at")).orderBy("course_credits", "id")
w_course_credits_enrolled_at_y_2 = Window.partitionBy("course_credits", F.year("enrolled_at")).orderBy("enrolled_at", "id").rowsBetween(-3, Window.currentRow)
w_course_credits_enrolled_at_y_3 = Window.partitionBy("course_credits", F.year("enrolled_at")).orderBy("enrolled_at", "id").rowsBetween(-6, Window.currentRow)
w_course_credits_enrolled_at_y_4 = Window.partitionBy("course_credits", F.year("enrolled_at")).orderBy("enrolled_at", "id")
w_student_id_course_level_by_s_3 = Window.partitionBy("student_id", "course_level").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_course_level_by_s_4 = Window.partitionBy("student_id", "course_level").orderBy(F.col("score").desc(), "id")

cleaned = (
    df_enrollments
    .filter(F.col("course_id") <= 4)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
)

with_details = (
    cleaned.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.course_code").isNotNull())
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        "c.course_code",
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
    )
)

sequenced = (
    with_details
    .select(
        "id",
        "student_id",
        "score",
        "enrolled_at",
        "course_code",
        "course_level",
        "course_credits",
        F.round(F.col("course_credits") / F.nullif(F.sum("course_credits").over(w_enrolled_at_month), F.lit(0)), 4).alias("course_credits_share_of_enrolled_at_month"),
        F.rank().over(w_student_id_course_level_by_s).alias("score_rank"),
        F.min("score").over(w_enrolled_at_month).alias("enrolled_at_month_min_score"),
        F.count(F.lit(1)).over(w_enrolled_at_month).alias("rows_in_enrolled_at_month"),
        F.first("course_credits").over(w_student_id_course_level_by_s_2).alias("first_course_credits"),
        F.size(F.collect_set("course_id").over(w_enrolled_at_month)).alias("distinct_course_id_in_enrolled_at_month"),
        F.sum("course_credits").over(w_course_credits_enrolled_at_y).alias("running_course_credits"),
        F.lag("course_credits", 2, 0).over(w_student_id_enrolled_at_year).alias("prev2_course_credits"),
        F.lead("enrolled_at").over(w_student_id_enrolled_at_year).alias("next_enrolled_at"),
        F.round(F.avg("course_credits").over(w_course_credits_enrolled_at_y_2), 2).alias("course_credits_avg_4"),
        F.round(F.percent_rank().over(w_student_id_course_level_by_s), 3).alias("score_percent_rank"),
        F.max("score").over(w_course_credits_enrolled_at_y_3).alias("score_max_7"),
        F.round(F.percent_rank().over(w_course_credits_enrolled_at_y_4), 3).alias("enrolled_at_percent_rank"),
        F.first("score", ignorenulls=True).over(w_student_id_course_level_by_s_3).alias("first_known_score"),
        F.count(F.lit(1)).over(w_course_credits_enrolled_at_y).alias("running_count"),
        F.dense_rank().over(w_student_id_course_level_by_s_4).alias("score_dense_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "student_id",
        "score",
        "enrolled_at",
        "course_code",
        "course_level",
        "course_credits",
        "enrolled_at_month_min_score",
        "rows_in_enrolled_at_month",
        "first_course_credits",
        "distinct_course_id_in_enrolled_at_month",
        "running_course_credits",
        "next_enrolled_at",
        "course_credits_avg_4",
        "score_percent_rank",
        "score_max_7",
        "enrolled_at_percent_rank",
        "running_count",
        "score_dense_rank",
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
    )
    .orderBy(F.col("course_credits_avg_4").desc(), "id")
    .limit(10)
)
