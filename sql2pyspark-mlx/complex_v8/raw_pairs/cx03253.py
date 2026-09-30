from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_major_cohort_by_gpa = Window.partitionBy("major", "cohort").orderBy("gpa", "id")
w_major_is_international_by_gp = Window.partitionBy("major", "is_international").orderBy(F.col("gpa").desc(), "id").rowsBetween(-2, Window.currentRow)
w_major_is_international_by_gp_2 = Window.partitionBy("major", "is_international").orderBy(F.col("gpa").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_major_cohort_by_gpa_prior_2 = Window.partitionBy("major", "cohort").orderBy("gpa", "id").rowsBetween(-2, -1)
w_major_cohort_by_gpa_running = Window.partitionBy("major", "cohort").orderBy("gpa", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_major_by_enrolled_on_moving = Window.partitionBy("major").orderBy("enrolled_on", "id").rowsBetween(-2, Window.currentRow)
w_is_international_cohort = Window.partitionBy("is_international", "cohort")
w_major_by_enrolled_on_full = Window.partitionBy("major").orderBy("enrolled_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_last_enrolled_on_month_major = Window.partitionBy(F.date_trunc("month", "last_enrolled_on"), "major").orderBy("days_since_last_enrolled_on", "id").rowsBetween(-6, Window.currentRow)
w_major_distinct_major_in_is_i = Window.partitionBy("major", "distinct_major_in_is_international_cohort").orderBy("gpa_sum_2_prior")
w_last_enrolled_on_month_major_2 = Window.partitionBy(F.date_trunc("month", "last_enrolled_on"), "major").orderBy("days_since_last_enrolled_on", "id")
w_is_international = Window.partitionBy("is_international")
w_last_enrolled_on_month_major_3 = Window.partitionBy(F.date_trunc("month", "last_enrolled_on"), "major").orderBy("days_since_last_enrolled_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_cohort = Window.partitionBy("cohort")
w_all_by_n_upper_trim_student = Window.orderBy("n_upper_trim_student_name_like", "last_enrolled_on_month", "cohort")
w_all_by_n_upper_trim_student_2 = Window.orderBy("n_upper_trim_student_name_like", "last_enrolled_on_month", "cohort").rowsBetween(-2, -1)

valid_students = (
    df_students
    .filter(F.col("cohort") != "C2023")
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.concat_ws(": ", F.col("major"), F.col("student_name")).alias("major_student_name_label"),
    )
)

ranked = (
    valid_students
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "is_international",
        "major_student_name_label",
        F.lag("enrolled_on", 2).over(w_major_cohort_by_gpa).alias("prev2_enrolled_on"),
        F.round(F.avg("gpa").over(w_major_is_international_by_gp), 2).alias("gpa_avg_3"),
        F.first("gpa", ignorenulls=True).over(w_major_is_international_by_gp_2).alias("first_known_gpa"),
        F.sum("gpa").over(w_major_cohort_by_gpa_prior_2).alias("gpa_sum_2_prior"),
        F.sum("gpa").over(w_major_cohort_by_gpa_running).alias("running_gpa"),
        F.round(F.avg("gpa").over(w_major_by_enrolled_on_moving), 2).alias("gpa_avg_3_2"),
        F.size(F.collect_set("major").over(w_is_international_cohort)).alias("distinct_major_in_is_international_cohort"),
        F.last("enrolled_on").over(w_major_by_enrolled_on_full).alias("last_enrolled_on"),
    )
)

derived = (
    ranked
    .withColumn("days_since_last_enrolled_on", F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_enrolled_on")))
)

metrics = (
    derived
    .select(
        "*",
        F.round(F.avg("first_known_gpa").over(w_last_enrolled_on_month_major), 2).alias("first_known_gpa_avg_7"),
        F.dense_rank().over(w_major_distinct_major_in_is_i).alias("gpa_sum_2_prior_dense_rank"),
        F.ntile(10).over(w_last_enrolled_on_month_major_2).alias("days_since_last_enrolled_on_decile"),
        F.count(F.lit(1)).over(w_is_international).alias("rows_in_is_international"),
        F.nth_value("gpa_avg_3_2", 2).over(w_last_enrolled_on_month_major_3).alias("second_gpa_avg_3_2"),
    )
)

summary = (
    metrics
    .groupBy(
        F.date_trunc("month", "last_enrolled_on").alias("last_enrolled_on_month"),
        "cohort",
    )
    .agg(
        F.sum(F.when(F.upper(F.trim(F.col("student_name"))).like("J%"), 1).otherwise(0)).alias("n_upper_trim_student_name_like"),
        F.round(100 * F.count(F.when(F.col("cohort") == "C2021", 1)) / F.count("*"), 1).alias("pct_cohort_c2021"),
    )
)

ranked_2 = (
    summary
    .select(
        "last_enrolled_on_month",
        "cohort",
        "n_upper_trim_student_name_like",
        "pct_cohort_c2021",
        F.count(F.lit(1)).over(w_cohort).alias("rows_in_cohort"),
        F.round(F.percent_rank().over(w_all_by_n_upper_trim_student), 3).alias("n_upper_trim_student_name_like_percent_rank"),
        F.sum("pct_cohort_c2021").over(w_all_by_n_upper_trim_student_2).alias("pct_cohort_c2021_sum_2_prior"),
    )
)

result = (
    ranked_2
    .select(
        "last_enrolled_on_month",
        "cohort",
        "n_upper_trim_student_name_like",
        "pct_cohort_c2021",
        "rows_in_cohort",
        "n_upper_trim_student_name_like_percent_rank",
        "pct_cohort_c2021_sum_2_prior",
        F.date_format(F.col("last_enrolled_on_month"), "yyyy-MM-dd HH:00").alias("last_enrolled_on_month_label"),
        F.to_date(F.col("last_enrolled_on_month")).alias("last_enrolled_on_month_date"),
    )
    .orderBy(
        F.col("last_enrolled_on_month_label").asc(),
        F.col("rows_in_cohort").asc(),
        "last_enrolled_on_month",
        "cohort",
    )
    .limit(20)
)
