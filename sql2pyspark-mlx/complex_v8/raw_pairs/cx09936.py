from pyspark.sql import functions as F

cleaned = (
    df_students
    .filter((F.col("enrolled_on") >= F.lit("2024-05-18").cast("date")) & (F.col("cohort") != "C2021"))
    .select(
        "id",
        "student_name",
        "cohort",
        "is_international",
        F.date_format(F.col("enrolled_on"), "yyyy-MM-dd").alias("enrolled_on_label"),
    )
)

tagged = (
    cleaned
    .select(
        "*",
        F.coalesce(F.col("cohort"), F.lit("UNKNOWN")).alias("cohort_or_unknown"),
    )
)

qualified = (
    tagged.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.grade_status") == "INCOMPLETE") & (F.col("e.enrolled_at") < F.lit("2025-02-18 08:00:00").cast("timestamp")), "left_anti")
)

per_group = (
    qualified
    .filter(F.col("cohort_or_unknown") == "C2022")
    .groupBy("is_international", "cohort_or_unknown", "cohort")
    .agg(
        F.count(F.when(~F.col("is_international"), 1)).alias("n_is_international_false"),
        F.count(F.when(F.upper(F.trim(F.col("student_name"))).like("C%"), 1)).alias("n_upper_trim_student_name_like"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("cohort_or_unknown").alias("distinct_cohort_or_unknowns"),
        F.array_sort(F.collect_set("student_name")).alias("student_name_list"),
    )
)

result = (
    per_group
    .groupBy("distinct_cohort_or_unknowns", "cohort")
    .pivot("cohort_or_unknown", ["C2024", "UNKNOWN"])
    .agg(F.max("cnt").alias("max_v"), F.count("cnt").alias("n"))
)
