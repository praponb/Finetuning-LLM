from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_international_major = Window.partitionBy("is_international", "major")
w_is_international_major_by_gp = Window.partitionBy("is_international", "major").orderBy(F.col("gpa").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_major_cohort_by_enrolled_on = Window.partitionBy("major", "cohort").orderBy(F.col("enrolled_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_cohort = Window.partitionBy("cohort")
w_cohort_by_gpa = Window.partitionBy("cohort").orderBy("gpa", "id")
w_cohort_by_gpa_running = Window.partitionBy("cohort").orderBy("gpa", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_students
    .filter(~F.col("is_international"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.lower(F.regexp_replace(F.trim(F.col("major")), " ", "_")).alias("major_slug"),
        F.substring(F.trim(F.col("major")), 1, 4).alias("major_prefix"),
    )
)

with_windows = (
    prepared
    .select(
        "*",
        F.count(F.lit(1)).over(w_is_international_major).alias("rows_in_is_international_major"),
        F.nth_value("gpa", 2).over(w_is_international_major_by_gp).alias("second_gpa"),
        F.last("gpa").over(w_major_cohort_by_enrolled_on).alias("last_gpa"),
        F.count(F.lit(1)).over(w_cohort).alias("rows_in_cohort"),
        F.round(F.col("gpa") - F.avg("gpa").over(w_is_international_major), 2).alias("gpa_vs_is_international_major_avg"),
        F.max("gpa").over(w_cohort).alias("cohort_max_gpa"),
        F.lead("enrolled_on", 2).over(w_cohort_by_gpa).alias("next2_enrolled_on"),
        F.ntile(5).over(w_cohort_by_gpa).alias("gpa_quintile"),
        F.first("gpa", ignorenulls=True).over(w_cohort_by_gpa_running).alias("first_known_gpa"),
        F.sum("gpa").over(w_cohort).alias("cohort_sum_gpa"),
    )
)

derived = (
    with_windows
    .select(
        "*",
        F.coalesce(F.col("cohort_max_gpa"), F.lit(0.0)).alias("cohort_max_gpa_filled"),
        F.abs(F.col("cohort_sum_gpa") - 20.8).alias("cohort_sum_gpa_distance"),
        F.coalesce(F.col("major"), F.lit("UNKNOWN")).alias("major_or_unknown"),
    )
)

result = (
    derived
    .select(
        "id",
        "cohort",
        "major",
        "gpa",
        "is_international",
        "major_slug",
        "rows_in_is_international_major",
        "next2_enrolled_on",
        "gpa_quintile",
        "first_known_gpa",
        "cohort_sum_gpa",
        "cohort_sum_gpa_distance",
        "major_or_unknown",
        F.concat_ws(" / ", F.col("major_or_unknown"), F.col("student_name")).alias("major_or_unknown_student_name_label"),
        F.year(F.col("enrolled_on")).alias("enrolled_on_year"),
    )
)
