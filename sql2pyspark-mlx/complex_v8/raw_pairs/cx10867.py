from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_major_cohort = Window.partitionBy("major", "cohort")
w_all_by_gpa = Window.orderBy("gpa", "id")
w_major_by_enrolled_on = Window.partitionBy("major").orderBy("enrolled_on", "id")
w_major_by_enrolled_on_prior_6 = Window.partitionBy("major").orderBy("enrolled_on", "id").rowsBetween(-6, -1)
w_major_by_gpa_running = Window.partitionBy("major").orderBy("gpa", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_major_is_international_by_en = Window.partitionBy("major", "is_international").orderBy(F.col("enrolled_on").desc(), "id")

scoped_students = (
    df_students
    .filter((F.col("major").isin("Math", "Biz")) & (F.upper(F.trim(F.col("student_name"))).like("C%")))
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

with_trends = (
    scoped_students
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        F.round(F.col("gpa") - F.avg("gpa").over(w_major_cohort), 2).alias("gpa_vs_major_cohort_avg"),
        F.ntile(5).over(w_all_by_gpa).alias("gpa_quintile"),
        (F.col("gpa") - F.lag("gpa").over(w_major_by_enrolled_on)).alias("gpa_change"),
        F.max("gpa").over(w_major_by_enrolled_on_prior_6).alias("gpa_max_6_prior"),
        F.ntile(3).over(w_all_by_gpa).alias("gpa_tercile"),
        F.first("gpa", ignorenulls=True).over(w_major_by_gpa_running).alias("first_known_gpa"),
        F.dense_rank().over(w_major_is_international_by_en).alias("enrolled_on_dense_rank"),
        F.round(F.avg("gpa").over(w_major_cohort), 2).alias("major_cohort_avg_gpa"),
    )
)

top_per_group = (
    with_trends
    .filter(F.col("enrolled_on_dense_rank") == 1)
    .drop("enrolled_on_dense_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "cohort",
        "major",
        "gpa",
        "gpa_vs_major_cohort_avg",
        "gpa_max_6_prior",
        "gpa_tercile",
        "major_cohort_avg_gpa",
        F.coalesce(F.col("gpa_max_6_prior"), F.lit(0.0)).alias("gpa_max_6_prior_filled"),
    )
    .orderBy(F.col("gpa").desc(), "id")
    .limit(10)
)
