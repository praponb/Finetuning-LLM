from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_gpa = Window.orderBy(F.col("gpa").desc(), "id")
w_all_by_enrolled_on_full = Window.orderBy("enrolled_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_cohort = Window.partitionBy("cohort")
w_all_by_enrolled_on_moving_2 = Window.orderBy("enrolled_on", "id").rowsBetween(-2, Window.currentRow)
w_is_international = Window.partitionBy("is_international")

cleaned_students = (
    df_students
    .filter(F.col("enrolled_on") >= F.lit("2024-03-15").cast("date"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.concat_ws(": ", F.col("major"), F.col("cohort")).alias("major_cohort_label"),
        F.when(F.col("cohort").isin("C2023", "C2021"), "group_a").when(F.col("cohort") == "C2022", "group_b").otherwise("other").alias("cohort_group"),
    )
)

windowed = (
    cleaned_students
    .select(
        "*",
        F.ntile(3).over(w_all_by_gpa).alias("gpa_tercile"),
        F.first("gpa").over(w_all_by_enrolled_on_full).alias("first_gpa"),
        F.first("enrolled_on").over(w_all_by_enrolled_on_full).alias("first_enrolled_on"),
        F.round(F.col("gpa") - F.avg("gpa").over(w_cohort), 2).alias("gpa_vs_cohort_avg"),
        F.round(F.avg("gpa").over(w_all_by_enrolled_on_moving_2), 2).alias("gpa_avg_3"),
        F.round(F.col("gpa") - F.avg("gpa").over(w_is_international), 2).alias("gpa_vs_is_international_avg"),
        F.ntile(4).over(w_all_by_gpa).alias("gpa_quartile"),
        F.round(F.col("gpa") / F.nullif(F.sum("gpa").over(w_cohort), F.lit(0)), 4).alias("gpa_share_of_cohort"),
    )
)

result = (
    windowed
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        "major_cohort_label",
        "cohort_group",
        "gpa_tercile",
        "first_gpa",
        "gpa_vs_cohort_avg",
        "gpa_avg_3",
        "gpa_quartile",
    )
    .orderBy("first_gpa", F.col("gpa_vs_cohort_avg").desc())
)
