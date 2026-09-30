from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_gpa_ties = Window.orderBy("gpa")
w_major = Window.partitionBy("major")
w_all_by_gpa = Window.orderBy("gpa", "id")

prepared_students = (
    df_students
    .filter((F.col("cohort") == "C2024") & (F.upper(F.trim(F.col("student_name"))).like("B%")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        F.abs(F.col("gpa") - 2.9).alias("gpa_distance"),
    )
)

with_windows = (
    prepared_students
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        F.dense_rank().over(w_all_by_gpa_ties).alias("gpa_dense_rank"),
        F.round(F.col("gpa_distance") / F.nullif(F.sum("gpa_distance").over(w_major), F.lit(0)), 4).alias("gpa_distance_share_of_major"),
        F.ntile(5).over(w_all_by_gpa).alias("gpa_quintile"),
        F.min("gpa").over(w_major).alias("major_min_gpa"),
        (F.col("gpa_distance") - F.lag("gpa_distance").over(w_all_by_gpa)).alias("gpa_distance_change"),
    )
)

derived = (
    with_windows
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "gpa",
        "gpa_dense_rank",
        "major_min_gpa",
        F.lower(F.regexp_replace(F.trim(F.col("cohort")), " ", "_")).alias("cohort_slug"),
        F.concat_ws("|", F.col("major"), F.col("cohort")).alias("major_cohort_label"),
    )
)

first_rows = (
    derived
    .filter(F.col("gpa_dense_rank") <= 5)
    .drop("gpa_dense_rank")
)

stats = (
    first_rows
    .groupBy(F.year("enrolled_on").alias("enrolled_on_year"), "major")
    .agg(
        F.max("major_min_gpa").alias("max_major_min_gpa"),
        F.percentile_approx("gpa", 0.5).alias("median_gpa"),
        F.round(100 * F.count(F.when(F.lower(F.col("student_name")).like("% c%"), 1)) / F.count("*"), 1).alias("pct_lower_student_name_like_c"),
        F.round(F.sum("major_min_gpa"), 2).alias("total_major_min_gpa"),
        F.sum("gpa").alias("sum_gpa_check"),
    )
    .filter(F.col("sum_gpa_check") > 2.0)
    .drop("sum_gpa_check")
)

screened = (
    stats
    .crossJoin(stats.agg(F.avg("max_major_min_gpa").alias("overall_avg_max_major_min_gpa")))
    .filter(F.col("max_major_min_gpa") > F.col("overall_avg_max_major_min_gpa"))
    .drop("overall_avg_max_major_min_gpa")
)

result = (
    screened
    .filter(F.col("pct_lower_student_name_like_c").between(5.0, 75.0))
    .select(
        "enrolled_on_year",
        "major",
        "max_major_min_gpa",
        "median_gpa",
        "pct_lower_student_name_like_c",
        "total_major_min_gpa",
        F.least(F.col("median_gpa"), F.col("max_major_min_gpa")).alias("least_median_gpa_max_major_min_gpa"),
    )
    .orderBy("pct_lower_student_name_like_c")
)
