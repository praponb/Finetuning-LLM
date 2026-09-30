from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_on_year_is_internat = Window.partitionBy(F.year("enrolled_on"), "is_international")
w_major_by_enrolled_on_prior_1 = Window.partitionBy("major").orderBy("enrolled_on", "id").rowsBetween(-1, -1)
w_is_international_major_by_gp = Window.partitionBy("is_international", "major").orderBy("gpa", "id")
w_cohort_by_enrolled_on = Window.partitionBy("cohort").orderBy(F.col("enrolled_on").desc(), "id")
w_major_by_enrolled_on_running = Window.partitionBy("major").orderBy("enrolled_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_major_enrolled_on_year_by_gp = Window.partitionBy("major", F.year("enrolled_on")).orderBy("gpa", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_major_enrolled_on_year_by_gp_2 = Window.partitionBy("major", F.year("enrolled_on")).orderBy("gpa", "id").rowsBetween(-3, Window.currentRow)
w_major_by_gpa_avg_1_prior_tie = Window.partitionBy("major").orderBy("gpa_avg_1_prior")
w_major_last_known_enrolled_on = Window.partitionBy("major", F.date_trunc("month", "last_known_enrolled_on"))
w_cohort_by_last_known_enrolle = Window.partitionBy("cohort").orderBy(F.col("last_known_enrolled_on_plus_30d").desc())
w_major_by_gpa_avg_1_prior_ful = Window.partitionBy("major").orderBy("gpa_avg_1_prior", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_major_by_gpa_avg_1_prior = Window.partitionBy("major").orderBy("gpa_avg_1_prior", "id")
w_cohort_by_last_known_enrolle_2 = Window.partitionBy("cohort").orderBy(F.col("last_known_enrolled_on_plus_30d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_students = (
    df_students
    .filter(F.col("gpa") <= 3.4)
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.lower(F.regexp_replace(F.trim(F.col("student_name")), " ", "_")).alias("student_name_slug"),
    )
)

with_windows = (
    recent_students
    .select(
        "*",
        F.round(F.avg("gpa").over(w_enrolled_on_year_is_internat), 2).alias("enrolled_on_year_is_international_avg_gpa"),
        F.round(F.avg("gpa").over(w_major_by_enrolled_on_prior_1), 2).alias("gpa_avg_1_prior"),
        F.round(F.cume_dist().over(w_is_international_major_by_gp), 3).alias("gpa_cume_dist"),
        F.lag("enrolled_on").over(w_is_international_major_by_gp).alias("prev_enrolled_on"),
        F.row_number().over(w_cohort_by_enrolled_on).alias("rn"),
        F.max("gpa").over(w_major_by_enrolled_on_running).alias("max_gpa_so_far"),
        F.last("enrolled_on", ignorenulls=True).over(w_major_enrolled_on_year_by_gp).alias("last_known_enrolled_on"),
        F.count(F.lit(1)).over(w_enrolled_on_year_is_internat).alias("rows_in_enrolled_on_year_is_international"),
        F.max("gpa").over(w_major_enrolled_on_year_by_gp_2).alias("gpa_max_4"),
    )
)

scored = (
    with_windows
    .filter(F.col("major").isNotNull())
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "student_name_slug",
        "gpa_avg_1_prior",
        "gpa_cume_dist",
        "max_gpa_so_far",
        "last_known_enrolled_on",
        "gpa_max_4",
        F.date_add(F.col("last_known_enrolled_on"), 30).alias("last_known_enrolled_on_plus_30d"),
    )
)

with_trends = (
    scored
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "gpa",
        "student_name_slug",
        "gpa_cume_dist",
        "max_gpa_so_far",
        "gpa_max_4",
        F.round(F.cume_dist().over(w_major_by_gpa_avg_1_prior_tie), 3).alias("gpa_avg_1_prior_cume_dist"),
        F.round(F.col("gpa_avg_1_prior") - F.avg("gpa_avg_1_prior").over(w_major_last_known_enrolled_on), 2).alias("gpa_avg_1_prior_vs_major_last_known_enrolled_on_"),
        F.round(F.percent_rank().over(w_cohort_by_last_known_enrolle), 3).alias("last_known_enrolled_on_plus_30d_percent_rank"),
        F.last("gpa").over(w_major_by_gpa_avg_1_prior_ful).alias("last_gpa"),
        F.row_number().over(w_major_by_gpa_avg_1_prior).alias("rn"),
        F.ntile(4).over(w_major_by_gpa_avg_1_prior).alias("gpa_avg_1_prior_quartile"),
        F.round(F.cume_dist().over(w_cohort_by_last_known_enrolle), 3).alias("last_known_enrolled_on_plus_30d_cume_dist"),
        F.nth_value("gpa_avg_1_prior", 2).over(w_cohort_by_last_known_enrolle_2).alias("second_gpa_avg_1_prior"),
    )
)

result = (
    with_trends
    .filter(F.col("gpa_cume_dist") <= 0.8)
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "gpa",
        "student_name_slug",
        "max_gpa_so_far",
        "gpa_avg_1_prior_cume_dist",
        "gpa_avg_1_prior_vs_major_last_known_enrolled_on_",
        "last_known_enrolled_on_plus_30d_percent_rank",
        "rn",
        "second_gpa_avg_1_prior",
    )
)
