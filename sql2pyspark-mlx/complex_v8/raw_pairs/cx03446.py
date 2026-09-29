from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_enrolled_at_dayofwee = Window.partitionBy("term").orderBy("enrolled_at_dayofweek")
w_all_by_enrolled_at_dayofweek = Window.orderBy("enrolled_at_dayofweek", "id")
w_enrolled_at_dayofweek_course = Window.partitionBy("enrolled_at_dayofweek", "course_level").orderBy(F.col("enrolled_at").desc(), "id")
w_enrolled_at_dayofweek_course_2 = Window.partitionBy("enrolled_at_dayofweek", "course_level").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-6, -1)
w_student_id_by_enrolled_at_da = Window.partitionBy("student_id").orderBy(F.col("enrolled_at_dayofweek").asc_nulls_last(), "id")
w_course_id_enrolled_at_dayofw = Window.partitionBy("course_id", "enrolled_at_dayofweek")
w_course_id_by_enrolled_at_day = Window.partitionBy("course_id").orderBy(F.col("enrolled_at_dayofweek").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_level_student_id_by_e = Window.partitionBy("course_level", "student_id").orderBy("enrolled_at", "id")
w_course_id_by_enrolled_at_day_2 = Window.partitionBy("course_id").orderBy(F.col("enrolled_at_dayofweek").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_dayofweek_2 = Window.orderBy("enrolled_at_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_dayofweek_course_3 = Window.partitionBy("enrolled_at_dayofweek", "course_level").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_enrollments = (
    df_enrollments
    .filter((F.col("term") == "2025T1") | (F.col("student_id") <= 9))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
    )
)

with_parent_course = (
    recent_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.enrolled_at_dayofweek",
        F.col("c.level").alias("course_level"),
        "c.course_code",
    )
)

ranked = (
    with_parent_course
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "enrolled_at_dayofweek",
        F.round(F.cume_dist().over(w_term_by_enrolled_at_dayofwee), 3).alias("enrolled_at_dayofweek_cume_dist"),
        F.lead("enrolled_at").over(w_all_by_enrolled_at_dayofweek).alias("next_enrolled_at"),
        F.rank().over(w_enrolled_at_dayofweek_course).alias("enrolled_at_rank"),
        F.max("enrolled_at_dayofweek").over(w_enrolled_at_dayofweek_course_2).alias("enrolled_at_dayofweek_max_6_prior"),
        F.row_number().over(w_enrolled_at_dayofweek_course).alias("seq_no"),
        (F.col("enrolled_at_dayofweek") - F.lag("enrolled_at_dayofweek").over(w_all_by_enrolled_at_dayofweek)).alias("enrolled_at_dayofweek_change"),
        F.lag("enrolled_at").over(w_student_id_by_enrolled_at_da).alias("prev_enrolled_at"),
        F.sum("enrolled_at_dayofweek").over(w_course_id_enrolled_at_dayofw).alias("course_id_enrolled_at_dayofweek_sum_enrolled_at_"),
        F.first("enrolled_at_dayofweek").over(w_course_id_by_enrolled_at_day).alias("first_enrolled_at_dayofweek"),
        F.ntile(10).over(w_course_level_student_id_by_e).alias("enrolled_at_decile"),
        F.sum("enrolled_at_dayofweek").over(w_course_id_by_enrolled_at_day_2).alias("running_enrolled_at_dayofweek"),
        F.nth_value("enrolled_at_dayofweek", 2).over(w_all_by_enrolled_at_dayofweek_2).alias("second_enrolled_at_dayofweek"),
        F.last("enrolled_at_dayofweek").over(w_enrolled_at_dayofweek_course_3).alias("last_enrolled_at_dayofweek"),
    )
)

result = (
    ranked
    .filter(F.col("enrolled_at_dayofweek_change") <= 3)
    .select(
        "id",
        "student_id",
        "enrolled_at_dayofweek_cume_dist",
        "next_enrolled_at",
        "enrolled_at_rank",
        "seq_no",
        "enrolled_at_dayofweek_change",
        "prev_enrolled_at",
        "course_id_enrolled_at_dayofweek_sum_enrolled_at_",
        "running_enrolled_at_dayofweek",
        "last_enrolled_at_dayofweek",
    )
    .orderBy("last_enrolled_at_dayofweek", F.col("next_enrolled_at").desc())
)
