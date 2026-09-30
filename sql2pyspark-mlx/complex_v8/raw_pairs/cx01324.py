from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_major_or_unknown_enrolled_on = Window.partitionBy("major_or_unknown", "enrolled_on_month").orderBy(F.col("gpa").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_major_major_or_unknown = Window.partitionBy("major", "major_or_unknown")

filtered = (
    df_students
    .filter((~F.col("is_international")) & (F.col("cohort") == "C2023"))
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "gpa",
        F.coalesce(F.col("major"), F.lit("UNKNOWN")).alias("major_or_unknown"),
        F.concat(F.col("cohort"), F.lit("#"), F.col("id").cast("string")).alias("cohort_ref"),
        F.concat(F.col("cohort"), F.lit("#"), F.col("id").cast("string")).alias("cohort_ref_2"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.month(F.col("enrolled_on")).alias("enrolled_on_month"),
        F.coalesce(F.col("major"), F.lit("UNKNOWN")).alias("major_or_unknown_2"),
        F.abs(F.col("gpa") - 3.2).alias("gpa_distance"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.last("enrolled_on").over(w_major_or_unknown_enrolled_on).alias("last_enrolled_on"),
        F.sum("gpa_distance").over(w_major_major_or_unknown).alias("major_major_or_unknown_sum_gpa_distance"),
        F.min("gpa").over(w_major_major_or_unknown).alias("major_major_or_unknown_min_gpa"),
    )
)

result = (
    metrics
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "cohort_ref",
        "cohort_ref_2",
        "enrolled_on_month",
        "gpa_distance",
        "last_enrolled_on",
        "major_major_or_unknown_sum_gpa_distance",
        "major_major_or_unknown_min_gpa",
        F.concat(F.col("major"), F.lit("#"), F.col("enrolled_on_month").cast("string")).alias("major_ref"),
        F.lower(F.trim(F.col("cohort_ref"))).alias("cohort_ref_clean"),
    )
)
