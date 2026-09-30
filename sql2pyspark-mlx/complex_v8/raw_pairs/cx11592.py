from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("grade_status", "term").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())

recent_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
    )
)

deduped = (
    recent_enrollments
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    deduped.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.col("c.dept").alias("course_dept"),
        "st.student_name",
        F.col("st.is_international").alias("student_is_international"),
    )
)

course_dept_stats = (
    joined
    .groupBy("course_dept")
    .agg(F.percentile_approx("score", 0.5).alias("median_score"))
)

compared = (
    joined.alias("s")
    .join(course_dept_stats.alias("g"), F.col("g.course_dept") == F.col("s.course_dept"), "inner")
    .filter(F.col("s.score") >= F.col("g.median_score"))
    .select(
        "s.*",
        "g.median_score",
        F.round(F.col("s.score") - F.col("g.median_score"), 2).alias("score_minus_median"),
    )
)

result = (
    compared
    .filter(F.col("score_minus_median") < 80.0)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "term_ref",
        "student_name",
        "student_is_international",
        "median_score",
        "score_minus_median",
        F.length(F.trim(F.col("course_dept"))).alias("course_dept_len"),
    )
)
