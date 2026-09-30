from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_course_id_by_cour = Window.partitionBy("student_id", "course_id").orderBy("course_credits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_credits = Window.partitionBy("course_credits")
w_all_by_course_credits = Window.orderBy(F.col("course_credits").desc(), "id")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_student_id_course_id_by_cour_2 = Window.partitionBy("student_id", "course_id").orderBy("course_credits", "id")
w_all_by_course_credits_full = Window.orderBy(F.col("course_credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_term = Window.partitionBy("student_id", "term")
w_term_course_credits_change = Window.partitionBy("term", "course_credits_change")
w_first_known_enrolled_at_mont = Window.partitionBy(F.date_trunc("month", "first_known_enrolled_at")).orderBy("student_id_term_avg_course_credits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_known_enrolled_at_mont_2 = Window.partitionBy(F.date_trunc("month", "first_known_enrolled_at")).orderBy("student_id_term_avg_course_credits", "id").rowsBetween(-3, -1)
w_first_known_enrolled_at_mont_3 = Window.partitionBy(F.date_trunc("month", "first_known_enrolled_at")).orderBy("student_id_term_avg_course_credits", "id")
w_enrolled_at_to_date_first_kn = Window.partitionBy(F.to_date("enrolled_at"), F.date_trunc("month", "first_known_enrolled_at")).orderBy("enrolled_at", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_enrollments
    .filter((F.col("grade_status") == "INCOMPLETE") & (F.col("score") > 13.9))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

combined_details_course = (
    base.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        F.col("c.credits").alias("course_credits"),
    )
)

sequenced = (
    combined_details_course
    .select(
        "*",
        F.first("enrolled_at", ignorenulls=True).over(w_student_id_course_id_by_cour).alias("first_known_enrolled_at"),
        F.round(F.col("course_credits") / F.nullif(F.sum("course_credits").over(w_course_credits), F.lit(0)), 4).alias("course_credits_share_of_course_credits"),
        F.lead("course_credits", 1, 0).over(w_all_by_course_credits).alias("next_course_credits"),
        (F.col("course_credits") - F.lag("course_credits").over(w_all_by_enrolled_at)).alias("course_credits_change"),
        F.ntile(3).over(w_all_by_course_credits).alias("course_credits_tercile"),
        F.rank().over(w_student_id_course_id_by_cour_2).alias("course_credits_rank"),
        F.nth_value("course_credits", 2).over(w_all_by_course_credits_full).alias("second_course_credits"),
        F.round(F.avg("course_credits").over(w_student_id_term), 2).alias("student_id_term_avg_course_credits"),
    )
)

metrics = (
    sequenced
    .select(
        "*",
        F.size(F.collect_set("course_id").over(w_term_course_credits_change)).alias("distinct_course_id_in_term_course_credits_change"),
        F.size(F.collect_set("grade_status").over(w_term_course_credits_change)).alias("distinct_grade_status_in_term_course_credits_cha"),
        F.last("course_credits", ignorenulls=True).over(w_first_known_enrolled_at_mont).alias("last_known_course_credits"),
        F.round(F.avg("course_credits").over(w_first_known_enrolled_at_mont_2), 2).alias("course_credits_avg_3_prior"),
        F.ntile(3).over(w_first_known_enrolled_at_mont_3).alias("student_id_term_avg_course_credits_tercile"),
        F.max("course_credits_tercile").over(w_enrolled_at_to_date_first_kn).alias("course_credits_tercile_max_4"),
    )
)

result = (
    metrics
    .filter(F.col("enrolled_at") < F.lit("2025-02-24 12:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "first_known_enrolled_at",
        "next_course_credits",
        "course_credits_change",
        "course_credits_tercile",
        "course_credits_rank",
        "second_course_credits",
        "student_id_term_avg_course_credits",
        "last_known_course_credits",
        "course_credits_avg_3_prior",
        "student_id_term_avg_course_credits_tercile",
        "course_credits_tercile_max_4",
        F.when(F.col("grade_status").isin("PASS", "WITHDRAWN"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
        F.date_format(F.col("first_known_enrolled_at"), "yyyy-MM-dd").alias("first_known_enrolled_at_label"),
    )
    .orderBy(F.col("enrolled_at").desc(), F.col("id").asc_nulls_last())
)
