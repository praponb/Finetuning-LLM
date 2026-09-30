from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cohort_or_unknown_is_interna = Window.partitionBy("cohort_or_unknown", "is_international")
w_cohort_len_by_enrolled_on_qu = Window.partitionBy("cohort_len").orderBy("enrolled_on_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_cohort_len_by_enrolled_on_qu_2 = Window.partitionBy("cohort_len").orderBy("enrolled_on_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_students = (
    df_students
    .filter((F.col("major").isin("Math", "Bio")) & (F.col("gpa") < 3.3))
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "gpa",
        "is_international",
        F.coalesce(F.col("gpa"), F.lit(0.0)).alias("gpa_filled"),
        F.coalesce(F.col("cohort"), F.lit("UNKNOWN")).alias("cohort_or_unknown"),
        F.year(F.col("enrolled_on")).alias("enrolled_on_year"),
    )
)

derived = (
    scoped_students
    .select(
        "*",
        F.length(F.trim(F.col("cohort"))).alias("cohort_len"),
        F.date_trunc("quarter", F.col("enrolled_on")).alias("enrolled_on_quarter"),
        F.least(F.col("gpa"), F.col("enrolled_on_year")).alias("least_gpa_enrolled_on_year"),
    )
)

with_trends = (
    derived
    .select(
        "*",
        F.count(F.lit(1)).over(w_cohort_or_unknown_is_interna).alias("rows_in_cohort_or_unknown_is_international"),
        F.max("gpa_filled").over(w_cohort_len_by_enrolled_on_qu).alias("max_gpa_filled_so_far"),
        F.nth_value("gpa_filled", 2).over(w_cohort_len_by_enrolled_on_qu_2).alias("second_gpa_filled"),
        F.round(F.avg("gpa_filled").over(w_cohort_or_unknown_is_interna), 2).alias("cohort_or_unknown_is_international_avg_gpa_fille"),
        F.last("gpa", ignorenulls=True).over(w_cohort_len_by_enrolled_on_qu).alias("last_known_gpa"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "gpa",
        "is_international",
        "gpa_filled",
        "cohort_or_unknown",
        "enrolled_on_year",
        "enrolled_on_quarter",
        "rows_in_cohort_or_unknown_is_international",
        "max_gpa_filled_so_far",
        "second_gpa_filled",
        "cohort_or_unknown_is_international_avg_gpa_fille",
        F.hour(F.col("enrolled_on_quarter")).alias("enrolled_on_quarter_hour"),
    )
    .orderBy("cohort", F.col("rows_in_cohort_or_unknown_is_international").asc())
)
