from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_major_bio_by_first_enrolle = Window.partitionBy("n_major_bio").orderBy("first_enrolled_on", "cohort")
w_n_rows_n_major_bio = Window.partitionBy("n_rows", "n_major_bio")
w_cohort_by_first_enrolled_on = Window.partitionBy("cohort").orderBy(F.col("first_enrolled_on").desc(), "cohort")
w_cohort_by_first_enrolled_on_2 = Window.partitionBy("cohort").orderBy(F.col("first_enrolled_on").desc(), "cohort").rowsBetween(-6, Window.currentRow)
w_n_major_bio_cohort = Window.partitionBy("n_major_bio", "cohort")
w_n_major_bio_by_first_enrolle_2 = Window.partitionBy("n_major_bio").orderBy(F.col("first_enrolled_on").desc(), "cohort").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_major_bio_by_first_enrolle_3 = Window.partitionBy("n_major_bio").orderBy(F.col("first_enrolled_on").desc(), "cohort")

valid = (
    df_students
    .filter(F.col("gpa").between(2.7, 3.3))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.round(F.col("gpa") * 0.9, 2).alias("gpa_adj"),
    )
)

grouped = (
    valid
    .groupBy("cohort")
    .agg(
        F.count("*").alias("n_rows"),
        F.count(F.when(F.col("major") == "Bio", 1)).alias("n_major_bio"),
        F.min("enrolled_on").alias("first_enrolled_on"),
    )
)

with_trends = (
    grouped
    .select(
        "cohort",
        "n_major_bio",
        "first_enrolled_on",
        F.lead("first_enrolled_on").over(w_n_major_bio_by_first_enrolle).alias("next_first_enrolled_on"),
        F.sum("n_major_bio").over(w_n_rows_n_major_bio).alias("n_rows_n_major_bio_sum_n_major_bio"),
    )
)

windowed = (
    with_trends
    .select(
        "cohort",
        "first_enrolled_on",
        F.lead("first_enrolled_on", 3).over(w_cohort_by_first_enrolled_on).alias("next3_first_enrolled_on"),
        F.round(F.avg("n_major_bio").over(w_cohort_by_first_enrolled_on_2), 2).alias("n_major_bio_avg_7"),
        F.round(F.col("n_major_bio") / F.nullif(F.sum("n_major_bio").over(w_n_major_bio_cohort), F.lit(0)), 4).alias("n_major_bio_share_of_n_major_bio_cohort"),
        F.last("next_first_enrolled_on").over(w_n_major_bio_by_first_enrolle_2).alias("last_next_first_enrolled_on"),
        F.lag("n_major_bio", 2).over(w_cohort_by_first_enrolled_on).alias("prev2_n_major_bio"),
        F.round(F.percent_rank().over(w_n_major_bio_by_first_enrolle_3), 3).alias("first_enrolled_on_percent_rank"),
    )
)

derived = (
    windowed
    .filter(F.col("n_major_bio_avg_7") >= 3.5)
    .select(
        "cohort",
        "first_enrolled_on",
        "next3_first_enrolled_on",
        "n_major_bio_avg_7",
        "n_major_bio_share_of_n_major_bio_cohort",
        "prev2_n_major_bio",
        "first_enrolled_on_percent_rank",
        F.round(F.col("prev2_n_major_bio") / F.nullif(F.col("n_major_bio_avg_7"), F.lit(0)), 3).alias("prev2_n_major_bio_per_n_major_bio_avg_7"),
    )
)

result = (
    derived
    .groupingSets(
        [["cohort", "prev2_n_major_bio"], ["cohort"], []],
        "cohort", "prev2_n_major_bio",
    )
    .agg(
        F.round(F.avg("n_major_bio_share_of_n_major_bio_cohort"), 2).alias("avg_n_major_bio_share_of_n_major_bio_cohort"),
        F.count(F.when(F.col("cohort") != "C2024", 1)).alias("n_cohort_c2024"),
    )
    .orderBy(
        F.col("cohort").asc_nulls_last(),
        F.col("prev2_n_major_bio").asc_nulls_last(),
    )
)
