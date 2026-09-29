from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id")
w_all_by_score_full = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_enrolled_on_month_st = Window.partitionBy(F.month("student_enrolled_on"), "student_major").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_enrolled_at_month = Window.partitionBy("course_id", F.date_trunc("month", "enrolled_at")).orderBy(F.col("student_enrolled_on").desc(), "id")
w_all_by_student_enrolled_on_f = Window.orderBy(F.col("student_enrolled_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_month_student_ma = Window.partitionBy(F.month("enrolled_at"), "student_major")
w_all_by_student_enrolled_on_r = Window.orderBy(F.col("student_enrolled_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_student_enrolled_on = Window.orderBy(F.col("student_enrolled_on").asc_nulls_last(), "id")
w_course_id_student_major_by_e = Window.partitionBy("course_id", "student_major").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_enrolled_at_month_2 = Window.partitionBy("course_id", F.date_trunc("month", "enrolled_at")).orderBy(F.col("student_enrolled_on").desc(), "id").rowsBetween(-6, -1)

valid = (
    df_enrollments
    .filter(F.col("grade_status") != "PASS")
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year"),
        F.abs(F.col("score") - 69.6).alias("score_distance"),
    )
)

joined_student = (
    valid.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        "st.student_name",
        F.col("st.major").alias("student_major"),
    )
)

windowed = (
    joined_student
    .select(
        "*",
        F.size(F.collect_set("course_id").over(w_student_id)).alias("distinct_course_id_in_student_id"),
        F.first("enrolled_at").over(w_all_by_score_full).alias("first_enrolled_at"),
        F.nth_value("enrolled_at", 2).over(w_student_enrolled_on_month_st).alias("second_enrolled_at"),
        F.row_number().over(w_course_id_enrolled_at_month).alias("rn"),
        F.nth_value("student_enrolled_on", 2).over(w_all_by_student_enrolled_on_f).alias("second_student_enrolled_on"),
        F.dense_rank().over(w_course_id_enrolled_at_month).alias("student_enrolled_on_dense_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
        F.size(F.collect_set("course_id").over(w_enrolled_at_month_student_ma)).alias("distinct_course_id_in_enrolled_at_month_student_"),
        F.size(F.collect_set("student_id").over(w_enrolled_at_month_student_ma)).alias("distinct_student_id_in_enrolled_at_month_student"),
        F.min("score").over(w_enrolled_at_month_student_ma).alias("enrolled_at_month_student_major_min_score"),
        F.first("student_enrolled_on", ignorenulls=True).over(w_all_by_student_enrolled_on_r).alias("first_known_student_enrolled_on"),
        F.rank().over(w_all_by_student_enrolled_on).alias("student_enrolled_on_rank"),
        F.nth_value("score", 2).over(w_course_id_student_major_by_e).alias("second_score"),
        F.max("score").over(w_course_id_enrolled_at_month_2).alias("score_max_6_prior"),
    )
)

leaders = windowed.filter(F.col("rn") <= 3)

rollup_base = (
    leaders
    .groupBy(
        F.year("second_enrolled_at").alias("second_enrolled_at_year"),
        "student_enrolled_on_rank",
        "distinct_student_id_in_enrolled_at_month_student",
    )
    .agg(
        F.min("rn").alias("min_rn"),
        F.max("score_max_6_prior").alias("max_score_max_6_prior"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    rollup_base
    .select(
        "second_enrolled_at_year",
        "student_enrolled_on_rank",
        "min_rn",
        "max_score_max_6_prior",
        "n_rows",
    )
    .orderBy(F.col("second_enrolled_at_year").desc())
)
