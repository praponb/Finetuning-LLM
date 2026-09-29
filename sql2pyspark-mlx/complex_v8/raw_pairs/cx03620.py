from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cohort = Window.partitionBy("cohort")

valid = (
    df_students
    .filter((F.upper(F.trim(F.col("student_name"))).like("G%")) & (F.col("cohort") != "C2024"))
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "gpa",
        F.add_months(F.col("enrolled_on"), 1).alias("enrolled_on_plus_1m"),
    )
)

sequenced = (
    valid
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "enrolled_on_plus_1m",
        F.count(F.lit(1)).over(w_cohort).alias("rows_in_cohort"),
        F.round(F.avg("gpa").over(w_cohort), 2).alias("cohort_avg_gpa"),
        F.round(F.col("gpa") - F.avg("gpa").over(w_cohort), 2).alias("gpa_vs_cohort_avg"),
        F.sum("gpa").over(w_cohort).alias("cohort_sum_gpa"),
    )
)

cohort_stats = (
    sequenced
    .groupBy("cohort")
    .agg(F.avg("rows_in_cohort").alias("avg_rows_in_cohort"))
)

compared = (
    sequenced.alias("s")
    .join(cohort_stats.alias("g"), F.col("g.cohort") == F.col("s.cohort"), "inner")
    .filter(F.col("s.rows_in_cohort") > F.col("g.avg_rows_in_cohort"))
    .select(
        "s.*",
        "g.avg_rows_in_cohort",
        F.round(F.col("s.rows_in_cohort") - F.col("g.avg_rows_in_cohort"), 2).alias("rows_in_cohort_minus_avg"),
    )
)

filtered = (
    compared
    .crossJoin(compared.agg(F.min("rows_in_cohort").alias("overall_min_rows_in_cohort")))
    .filter(F.col("rows_in_cohort") > F.col("overall_min_rows_in_cohort"))
    .drop("overall_min_rows_in_cohort")
)

result = (
    filtered
    .filter(F.col("rows_in_cohort") >= 5)
    .select(
        "id",
        "student_name",
        "cohort",
        "rows_in_cohort",
        "cohort_avg_gpa",
        "avg_rows_in_cohort",
        "rows_in_cohort_minus_avg",
    )
)
