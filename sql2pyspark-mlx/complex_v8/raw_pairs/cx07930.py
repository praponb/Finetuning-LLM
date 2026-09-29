from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_term_by_enrolled_a = Window.partitionBy("course_id", "term").orderBy("enrolled_at")
w_grade_status_term_by_enrolle = Window.partitionBy("grade_status", "term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_enrolled_at_year_b = Window.partitionBy("course_id", F.year("enrolled_at")).orderBy("enrolled_at")
w_course_id_by_enrolled_at_tie = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc())
w_enrolled_at_to_date_grade_st = Window.partitionBy(F.to_date("enrolled_at"), "grade_status").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_enrolled_at_year_b_2 = Window.partitionBy("course_id", F.year("enrolled_at")).orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_by_enrolled_at_ful = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_enrolled_at_year_b_3 = Window.partitionBy("course_id", F.year("enrolled_at")).orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_term_by_enrolled_a_2 = Window.partitionBy("course_id", "term").orderBy("enrolled_at", "id")

base = (
    df_enrollments
    .filter((F.col("grade_status").isin("FAIL", "WITHDRAWN")) & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.round(F.cume_dist().over(w_course_id_term_by_enrolled_a), 3).alias("enrolled_at_cume_dist"),
        F.last("enrolled_at", ignorenulls=True).over(w_grade_status_term_by_enrolle).alias("last_known_enrolled_at"),
        F.round(F.cume_dist().over(w_course_id_enrolled_at_year_b), 3).alias("enrolled_at_cume_dist_2"),
        F.rank().over(w_course_id_by_enrolled_at_tie).alias("enrolled_at_rank"),
        F.nth_value("enrolled_at", 2).over(w_enrolled_at_to_date_grade_st).alias("second_enrolled_at"),
        F.nth_value("enrolled_at", 2).over(w_course_id_enrolled_at_year_b_2).alias("second_enrolled_at_2"),
        F.first("enrolled_at").over(w_course_id_by_enrolled_at_ful).alias("first_enrolled_at"),
        F.last("enrolled_at", ignorenulls=True).over(w_course_id_enrolled_at_year_b_3).alias("last_known_enrolled_at_2"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_course_id_term_by_enrolled_a_2))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
    )
)

result = (
    metrics
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "term_ref",
        "last_known_enrolled_at",
        "enrolled_at_cume_dist_2",
        "enrolled_at_rank",
        "second_enrolled_at",
        "first_enrolled_at",
        "last_known_enrolled_at_2",
        "minutes_since_prev_enrolled_at",
        F.year(F.col("second_enrolled_at_2")).alias("second_enrolled_at_2_year"),
        (F.floor(F.col("enrolled_at_rank") / 2) * 2).alias("enrolled_at_rank_bucket"),
    )
    .orderBy(
        F.col("enrolled_at_rank").desc(),
        F.col("last_known_enrolled_at_2").desc(),
        "id",
    )
    .limit(20)
)
