from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_on_year_by_gpa_dist = Window.partitionBy(F.year("enrolled_on")).orderBy(F.col("gpa_distance").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_major_cohort = Window.partitionBy("major", "cohort")
w_enrolled_on_year_is_internat = Window.partitionBy(F.year("enrolled_on"), "is_international").orderBy(F.col("gpa_distance").desc(), "id")
w_enrolled_on_year_is_internat_2 = Window.partitionBy(F.year("enrolled_on"), "is_international").orderBy(F.col("gpa_distance").desc(), "id").rowsBetween(-2, Window.currentRow)
w_cohort_major_by_gpa_running = Window.partitionBy("cohort", "major").orderBy(F.col("gpa").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_on_year_by_gpa_dist_2 = Window.partitionBy(F.year("enrolled_on")).orderBy(F.col("gpa_distance").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_cohort_major_by_gpa_full = Window.partitionBy("cohort", "major").orderBy(F.col("gpa").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_cohort_major_by_gpa = Window.partitionBy("cohort", "major").orderBy(F.col("gpa").asc_nulls_last(), "id")

recent = (
    df_students
    .filter(F.lower(F.col("student_name")).like("%an%"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.abs(F.col("gpa") - 3.3).alias("gpa_distance"),
    )
)

with_windows = (
    recent
    .select(
        "id",
        "cohort",
        "enrolled_on",
        "is_international",
        "gpa_distance",
        F.max("gpa_distance").over(w_enrolled_on_year_by_gpa_dist).alias("gpa_distance_max_3"),
        F.round(F.avg("gpa").over(w_enrolled_on_year_by_gpa_dist), 2).alias("gpa_avg_3"),
        F.count(F.lit(1)).over(w_major_cohort).alias("rows_in_major_cohort"),
        F.round(F.col("gpa_distance") - F.avg("gpa_distance").over(w_major_cohort), 2).alias("gpa_distance_vs_major_cohort_avg"),
        F.lag("gpa").over(w_enrolled_on_year_is_internat).alias("prev_gpa"),
        F.lag("gpa", 2).over(w_enrolled_on_year_is_internat).alias("prev2_gpa"),
        F.round(F.avg("gpa_distance").over(w_enrolled_on_year_is_internat_2), 2).alias("gpa_distance_avg_3"),
        F.last("enrolled_on", ignorenulls=True).over(w_cohort_major_by_gpa_running).alias("last_known_enrolled_on"),
        F.sum("gpa_distance").over(w_enrolled_on_year_by_gpa_dist_2).alias("running_gpa_distance"),
        F.nth_value("gpa", 2).over(w_cohort_major_by_gpa_full).alias("second_gpa"),
        (F.col("gpa") - F.lag("gpa").over(w_cohort_major_by_gpa)).alias("gpa_change"),
    )
)

per_group = (
    with_windows
    .groupBy(
        "is_international",
        F.date_format("last_known_enrolled_on", "yyyy-MM").alias("last_known_enrolled_on_ym"),
        F.year("enrolled_on").alias("enrolled_on_year"),
    )
    .agg(
        F.max("rows_in_major_cohort").alias("max_rows_in_major_cohort"),
        F.count("*").alias("cnt"),
        F.countDistinct("cohort").alias("distinct_cohorts"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    per_group
    .filter(F.col("max_rows_in_major_cohort") > 5)
    .select(
        "is_international",
        "last_known_enrolled_on_ym",
        "max_rows_in_major_cohort",
        "cnt",
        "distinct_cohorts",
        "distinct_ids",
        F.when(F.col("distinct_ids") >= 6, "large").when(F.col("distinct_ids") >= 5, "medium").otherwise("small").alias("distinct_ids_tier"),
    )
)
