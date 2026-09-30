from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_major_by_enrolled_on = Window.partitionBy("major").orderBy(F.col("enrolled_on").desc(), "id")
w_cohort_by_days_since_enrolle = Window.partitionBy("cohort").orderBy(F.col("days_since_enrolled_on").desc(), "id").rowsBetween(-6, Window.currentRow)
w_cohort_by_days_since_enrolle_2 = Window.partitionBy("cohort").orderBy(F.col("days_since_enrolled_on").desc(), "id")
w_major = Window.partitionBy("major")
w_major_by_enrolled_on_prior_2 = Window.partitionBy("major").orderBy(F.col("enrolled_on").desc(), "id").rowsBetween(-2, -1)

prepared_students = (
    df_students
    .filter((F.col("gpa") <= 3.4) & (F.col("is_international")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_on")).alias("days_since_enrolled_on"),
        F.concat_ws(": ", F.col("major"), F.col("cohort")).alias("major_cohort_label"),
    )
)

windowed = (
    prepared_students
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        "days_since_enrolled_on",
        "major_cohort_label",
        F.round(F.percent_rank().over(w_major_by_enrolled_on), 3).alias("enrolled_on_percent_rank"),
        F.max("gpa").over(w_cohort_by_days_since_enrolle).alias("gpa_max_7"),
        F.row_number().over(w_cohort_by_days_since_enrolle_2).alias("seq_no"),
        F.round(F.col("days_since_enrolled_on") / F.nullif(F.sum("days_since_enrolled_on").over(w_major), F.lit(0)), 4).alias("days_since_enrolled_on_share_of_major"),
        F.round(F.avg("gpa").over(w_major_by_enrolled_on_prior_2), 2).alias("gpa_avg_2_prior"),
    )
)

labeled = windowed.withColumn("enrolled_on_label", F.date_format(F.col("enrolled_on"), "MMM yyyy"))

top_per_group = (
    labeled
    .filter((F.col("seq_no") <= 3) & (F.col("days_since_enrolled_on_share_of_major") < 0.8))
    .drop("seq_no")
)

result = (
    top_per_group
    .groupingSets(
        [["major", "cohort"], ["major"], ["cohort"], []],
        "major", "cohort",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("major").alias("distinct_majors"),
    )
    .orderBy(F.col("major").asc_nulls_last(), F.col("cohort").asc_nulls_last())
)
