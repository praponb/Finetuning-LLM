from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("cohort").orderBy(F.col("gpa").desc(), F.col("id").desc())
w_cohort_distinct_cohorts = Window.partitionBy("cohort", "distinct_cohorts")
w_all_by_stddev_gpa_full = Window.orderBy(F.col("stddev_gpa").desc(), "major", "cohort").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_students
    .filter(F.upper(F.trim(F.col("student_name"))).like("D%"))
    .select(
        "id",
        "cohort",
        "major",
        "gpa",
        "is_international",
        F.when(F.col("is_international"), "yes").otherwise("no").alias("is_international_label"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

totals = (
    latest
    .groupBy("major", "cohort")
    .agg(
        F.sum(F.when(F.col("cohort") == "C2024", F.col("gpa"))).alias("gpa_cohort_c2024"),
        F.array_sort(F.collect_set("major")).alias("major_list"),
        F.countDistinct("major").alias("distinct_majors"),
        F.countDistinct("cohort").alias("distinct_cohorts"),
        F.round(F.stddev("gpa"), 2).alias("stddev_gpa"),
    )
)

windowed = (
    totals
    .select(
        "major",
        "cohort",
        "gpa_cohort_c2024",
        "major_list",
        "stddev_gpa",
        F.size(F.collect_set("major").over(w_cohort_distinct_cohorts)).alias("distinct_major_in_cohort_distinct_cohorts"),
        F.nth_value("gpa_cohort_c2024", 2).over(w_all_by_stddev_gpa_full).alias("second_gpa_cohort_c2024"),
        F.first("distinct_cohorts").over(w_all_by_stddev_gpa_full).alias("first_distinct_cohorts"),
    )
)

result = (
    windowed
    .filter(F.col("gpa_cohort_c2024") <= 13.0)
    .select(
        "major",
        "cohort",
        "gpa_cohort_c2024",
        "major_list",
        "stddev_gpa",
        "second_gpa_cohort_c2024",
        "first_distinct_cohorts",
    )
    .orderBy(F.col("major").asc(), "stddev_gpa", "cohort")
    .limit(10)
)
