from pyspark.sql import functions as F

scoped_students = (
    df_students
    .filter((F.lower(F.col("student_name")).like("%sc%")) & (~F.col("is_international")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.substring(F.trim(F.col("student_name")), 1, 4).alias("student_name_prefix"),
        F.round(F.col("gpa") * 1.5, 2).alias("gpa_adj"),
    )
)

cohort_stats = (
    scoped_students
    .groupBy("cohort")
    .agg(F.avg("gpa_adj").alias("avg_gpa_adj"))
)

vs_group = (
    scoped_students.alias("s")
    .join(cohort_stats.alias("g"), F.col("g.cohort") == F.col("s.cohort"), "inner")
    .filter(F.col("s.gpa_adj") > F.col("g.avg_gpa_adj"))
    .select(
        "s.*",
        "g.avg_gpa_adj",
        F.round(F.col("s.gpa_adj") - F.col("g.avg_gpa_adj"), 2).alias("gpa_adj_minus_avg"),
    )
)

result = (
    vs_group
    .groupBy("is_international", "enrolled_on")
    .pivot("cohort", ["C2022", "C2023"])
    .agg(F.min("gpa"))
    .orderBy("is_international", "enrolled_on")
)
