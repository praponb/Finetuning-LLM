from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())
w_course_id = Window.partitionBy("course_id")
w_all_by_enrolled_at_running = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.substring(F.trim(F.col("term")), 1, 3).alias("term_prefix"),
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
    )
)

latest = (
    scoped_enrollments
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

screened = (
    latest.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (~F.col("st.is_international")) & (F.col("st.major").isNotNull()), "left_semi")
)

with_details_course = (
    screened.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.level") == "core"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.col("c.credits").alias("course_credits"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

term_stats = (
    with_details_course
    .groupBy("term")
    .agg(F.max("score").alias("max_score"))
)

vs_group = (
    with_details_course.alias("s")
    .join(term_stats.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.score") == F.col("g.max_score"))
    .select(
        "s.*",
        "g.max_score",
        F.round(F.col("s.score") - F.col("g.max_score"), 2).alias("score_minus_max"),
    )
)

sequenced = (
    vs_group
    .withColumn("enrolled_at_dayofweek_vs_course_id_avg", F.round(F.col("enrolled_at_dayofweek") - F.avg("enrolled_at_dayofweek").over(w_course_id), 2))
    .withColumn("first_known_max_score", F.first("max_score", ignorenulls=True).over(w_all_by_enrolled_at_running))
)

rollup_base = (
    sequenced
    .groupBy(
        "enrolled_at_dayofweek",
        F.when(F.col("score") >= 77.0, "HIGH").when(F.col("score") >= 13.9, "MEDIUM").otherwise("LOW").alias("score_tier"),
        F.year("enrolled_at").alias("enrolled_at_year"),
    )
    .agg(
        F.max("score_minus_max").alias("max_score_minus_max"),
        F.sum(F.when(F.col("student_cohort") == "C2024", 1).otherwise(0)).alias("n_student_cohort_c2024"),
        F.min("enrolled_at_dayofweek").alias("min_enrolled_at_dayofweek"),
    )
)

result = (
    rollup_base
    .filter(F.col("max_score_minus_max") < 80.0)
    .select(
        "enrolled_at_dayofweek",
        "score_tier",
        "enrolled_at_year",
        "n_student_cohort_c2024",
        "min_enrolled_at_dayofweek",
        F.substring(F.trim(F.col("score_tier")), 1, 3).alias("score_tier_prefix"),
        F.greatest(F.col("enrolled_at_year"), F.col("n_student_cohort_c2024")).alias("greatest_enrolled_at_year_n_student_cohort_c2024"),
    )
    .orderBy("score_tier_prefix")
)
