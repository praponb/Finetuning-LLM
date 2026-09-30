from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_major_is_international_by_gp = Window.partitionBy("major", "is_international").orderBy("gpa", "id")
w_major = Window.partitionBy("major")
w_major_is_international_by_gp_2 = Window.partitionBy("major", "is_international").orderBy("gpa")
w_cohort_enrolled_on_year_by_g = Window.partitionBy("cohort", F.year("enrolled_on")).orderBy(F.col("gpa").desc(), "id")
w_cohort_enrolled_on_year_by_g_2 = Window.partitionBy("cohort", F.year("enrolled_on")).orderBy(F.col("gpa").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_cohort = Window.partitionBy("cohort")
w_is_international_by_gpa_dens = Window.partitionBy("is_international").orderBy("gpa_dense_rank", "id")
w_is_international_by_max_gpa = Window.partitionBy("is_international").orderBy("max_gpa_so_far", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_students
    .filter((F.col("is_international")) & (F.col("major") != "Math"))
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

ranked = (
    base
    .select(
        "*",
        F.dense_rank().over(w_major_is_international_by_gp).alias("gpa_dense_rank"),
        F.count(F.lit(1)).over(w_major).alias("rows_in_major"),
        F.rank().over(w_major_is_international_by_gp_2).alias("gpa_rank"),
        F.datediff(F.col("enrolled_on"), F.lag("enrolled_on").over(w_cohort_enrolled_on_year_by_g)).alias("days_since_prev_enrolled_on"),
        F.max("gpa").over(w_cohort_enrolled_on_year_by_g_2).alias("max_gpa_so_far"),
    )
)

with_activity = (
    ranked
    .crossJoin(ranked.agg(F.max("gpa").alias("overall_max_gpa")))
    .filter(F.col("gpa") < F.col("overall_max_gpa"))
    .drop("overall_max_gpa")
)

ranked_2 = (
    with_activity
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        "gpa_dense_rank",
        "rows_in_major",
        "gpa_rank",
        "days_since_prev_enrolled_on",
        "max_gpa_so_far",
        F.round(F.col("max_gpa_so_far") - F.avg("max_gpa_so_far").over(w_cohort), 2).alias("max_gpa_so_far_vs_cohort_avg"),
        F.lead("enrolled_on").over(w_is_international_by_gpa_dens).alias("next_enrolled_on"),
        F.round(F.avg("gpa_dense_rank").over(w_is_international_by_max_gpa), 2).alias("gpa_dense_rank_avg_3"),
    )
)

with_activity_2 = (
    ranked_2.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.course_id").isNotNull()), "left_semi")
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.sum("score").alias("total_enrollment_score"),
    )
)

profiled = (
    with_activity_2.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at", "e.total_enrollment_score")
)

result = (
    profiled
    .filter(F.col("gpa_dense_rank_avg_3") >= 2.4)
    .select(
        "id",
        "cohort",
        "major",
        "is_international",
        "rows_in_major",
        "gpa_rank",
        "max_gpa_so_far",
        "max_gpa_so_far_vs_cohort_avg",
        "next_enrolled_on",
        "gpa_dense_rank_avg_3",
        "last_enrollment_enrolled_at",
        F.add_months(F.col("enrolled_on"), 12).alias("enrolled_on_plus_12m"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
