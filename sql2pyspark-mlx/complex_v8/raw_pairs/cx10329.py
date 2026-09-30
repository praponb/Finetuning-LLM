from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cohort_by_gpa_distance_movin = Window.partitionBy("cohort").orderBy("gpa_distance", "id").rowsBetween(-2, Window.currentRow)
w_is_international_cohort_len = Window.partitionBy("is_international", "cohort_len")
w_cohort_len_by_gpa_distance = Window.partitionBy("cohort_len").orderBy(F.col("gpa_distance").desc(), "id")

filtered_students = (
    df_students
    .filter((F.col("gpa") < 3.2) & (F.upper(F.trim(F.col("student_name"))).like("B%")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.abs(F.col("gpa") - 2.7).alias("gpa_distance"),
        F.length(F.trim(F.col("cohort"))).alias("cohort_len"),
        F.concat(F.col("cohort"), F.lit("#"), F.col("id").cast("string")).alias("cohort_ref"),
    )
)

with_activity = (
    filtered_students
    .crossJoin(filtered_students.agg(F.min("cohort_len").alias("overall_min_cohort_len")))
    .filter(F.col("cohort_len") > F.col("overall_min_cohort_len"))
    .drop("overall_min_cohort_len")
)

with_windows = (
    with_activity
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        "gpa_distance",
        "cohort_len",
        F.round(F.avg("gpa_distance").over(w_cohort_by_gpa_distance_movin), 2).alias("gpa_distance_avg_3"),
        F.round(F.col("gpa_distance") - F.avg("gpa_distance").over(w_is_international_cohort_len), 2).alias("gpa_distance_vs_is_international_cohort_len_avg"),
        F.lead("gpa_distance", 2).over(w_cohort_len_by_gpa_distance).alias("next2_gpa_distance"),
    )
)

result = (
    with_windows
    .cube("cohort", "is_international")
    .agg(
        F.round(F.sum("gpa_distance_avg_3"), 2).alias("total_gpa_distance_avg_3"),
        F.round(F.sum("gpa_distance_avg_3") / F.nullif(F.sum("cohort_len"), F.lit(0)), 3).alias("gpa_distance_avg_3_to_cohort_len_ratio"),
        F.countDistinct("major").alias("distinct_majors"),
    )
    .select(
        F.coalesce(F.col("cohort"), F.lit("ALL")).alias("cohort"),
        "is_international",
        "total_gpa_distance_avg_3",
        "gpa_distance_avg_3_to_cohort_len_ratio",
        "distinct_majors",
    )
    .orderBy(
        F.col("cohort").asc_nulls_last(),
        F.col("is_international").asc_nulls_last(),
    )
)
