from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_course_dept_by_sc = Window.partitionBy("student_id", "course_dept").orderBy("score_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_course_dept_by_sc_2 = Window.partitionBy("student_id", "course_dept").orderBy("score_filled", "id")
w_student_id = Window.partitionBy("student_id")

base = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
    )
)

with_parent_course = (
    base.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("c.level") == "core")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at_date",
        F.col("c.level").alias("course_level"),
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

scored = (
    with_parent_course
    .select(
        "*",
        F.when(F.col("course_level").isin("intro", "advanced"), "group_a").when(F.col("course_level") == "core", "group_b").otherwise("other").alias("course_level_group"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.when(F.col("score") >= 57.6, "HIGH").when(F.col("score") >= 11.3, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

windowed = (
    scored
    .select(
        "*",
        F.last("score_filled", ignorenulls=True).over(w_student_id_course_dept_by_sc).alias("last_known_score_filled"),
        F.dense_rank().over(w_student_id_course_dept_by_sc_2).alias("score_filled_dense_rank"),
        F.round(F.col("score_filled") - F.avg("score_filled").over(w_student_id), 2).alias("score_filled_vs_student_id_avg"),
    )
)

result = (
    windowed
    .filter(F.col("course_id") <= 5)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at_date",
        "course_level",
        "course_level_group",
        "score_filled",
        "score_tier",
        "score_filled_dense_rank",
        F.when(F.col("student_is_international"), 1).otherwise(0).alias("student_is_international_flag"),
    )
    .orderBy(F.col("grade_status").asc(), F.col("course_id").desc(), "id")
    .limit(10)
)
