from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_level = Window.partitionBy("course_level")
w_course_level_course_id_by_en = Window.partitionBy("course_level", "course_id").orderBy(F.col("enrolled_at_hour").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_enrolled_at_hour = Window.partitionBy("course_id", "enrolled_at_hour")
w_course_id_student_id_by_scor = Window.partitionBy("course_id", "student_id").orderBy("score")
w_course_level_course_id_by_en_2 = Window.partitionBy("course_level", "course_id").orderBy(F.col("enrolled_at_hour").desc(), "id")

recent = (
    df_enrollments
    .filter((F.col("student_id") <= 4) & (F.col("grade_status") != "PASS"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
    )
)

combined_details = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at_hour",
        F.col("c.level").alias("course_level"),
    )
)

ranked = (
    combined_details
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        F.round(F.col("score") - F.avg("score").over(w_course_level), 2).alias("score_vs_course_level_avg"),
        F.last("score").over(w_course_level_course_id_by_en).alias("last_score"),
        F.size(F.collect_set("course_level").over(w_course_id_enrolled_at_hour)).alias("distinct_course_level_in_course_id_enrolled_at_h"),
        F.dense_rank().over(w_course_id_student_id_by_scor).alias("score_dense_rank"),
        F.count(F.lit(1)).over(w_course_id_enrolled_at_hour).alias("rows_in_course_id_enrolled_at_hour"),
        F.max("score").over(w_course_level).alias("course_level_max_score"),
        F.lead("score", 2, 0).over(w_course_level_course_id_by_en_2).alias("next2_score"),
        F.round(F.col("enrolled_at_hour") / F.nullif(F.sum("enrolled_at_hour").over(w_course_level), F.lit(0)), 4).alias("enrolled_at_hour_share_of_course_level"),
    )
)

result = (
    ranked
    .select(
        "id",
        "course_id",
        "score_vs_course_level_avg",
        "score_dense_rank",
        "next2_score",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.round(F.col("next2_score") * 0.25, 2).alias("next2_score_adj"),
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("score_vs_course_level_avg").desc_nulls_first(),
    )
)
