from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_international_cohort = Window.partitionBy("is_international", "cohort")
w_cohort_by_gpa_ties = Window.partitionBy("cohort").orderBy(F.col("gpa").desc())

valid_students = (
    df_students
    .filter(F.col("gpa") < 3.3)
    .select(
        "id",
        "cohort",
        "gpa",
        "is_international",
        F.substring(F.trim(F.col("major")), 1, 4).alias("major_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("cohort")), " ", "_")).alias("cohort_slug"),
        F.initcap(F.trim(F.col("cohort"))).alias("cohort_clean"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

with_counts = (
    valid_students.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

metrics = (
    with_counts
    .select(
        "*",
        F.min("gpa").over(w_is_international_cohort).alias("is_international_cohort_min_gpa"),
        F.round(F.cume_dist().over(w_cohort_by_gpa_ties), 3).alias("gpa_cume_dist"),
    )
)

result = (
    metrics
    .groupingSets(
        [["cohort", "is_international"], ["cohort"], ["is_international"], []],
        "cohort", "is_international",
    )
    .agg(
        F.round(F.avg("gpa_cume_dist"), 2).alias("avg_gpa_cume_dist"),
        F.round(100 * F.count(F.when(~F.col("cohort").isin("C2021", "C2023"), 1)) / F.count("*"), 1).alias("pct_cohort_not_in_c2021_c2023"),
        F.max("gpa_cume_dist").alias("max_gpa_cume_dist"),
    )
    .orderBy(
        F.col("cohort").asc_nulls_last(),
        F.col("is_international").asc_nulls_last(),
    )
)
