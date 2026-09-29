from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at_plus_7d_m = Window.orderBy("enrolled_at_plus_7d", "id").rowsBetween(-3, Window.currentRow)
w_all_by_enrolled_at_plus_7d = Window.orderBy("enrolled_at_plus_7d", "id")
w_course_id = Window.partitionBy("course_id")
w_all_by_enrolled_at_plus_7d_t = Window.orderBy("enrolled_at_plus_7d")

recent_enrollments = (
    df_enrollments
    .filter((F.col("grade_status") == "WITHDRAWN") & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.col("enrolled_at") + F.expr("INTERVAL 7 DAYS")).alias("enrolled_at_plus_7d"),
        F.concat_ws(": ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

with_details = (
    recent_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at_plus_7d",
        "s.term_grade_status_label",
        F.col("c.level").alias("course_level"),
        "c.course_code",
        "st.student_name",
    )
)

sequenced = (
    with_details
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "course_level",
        "student_name",
        F.max("score").over(w_all_by_enrolled_at_plus_7d_m).alias("score_max_4"),
        F.lag("score").over(w_all_by_enrolled_at_plus_7d).alias("prev_score"),
        F.dense_rank().over(w_all_by_enrolled_at_plus_7d).alias("enrolled_at_plus_7d_dense_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at_plus_7d_t), 3).alias("enrolled_at_plus_7d_percent_rank"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
    )
)

result = (
    sequenced
    .rollup("student_id", "course_id", "grade_status")
    .agg(
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.round(F.avg("score_max_4"), 2).alias("avg_score_max_4"),
        F.sum(F.when(F.length(F.trim(F.col("student_name"))) >= 5, F.col("enrolled_at_plus_7d_percent_rank")).otherwise(0)).alias("enrolled_at_plus_7d_percent_rank_length_trim_stu"),
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
    )
)
