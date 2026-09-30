from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_gpa = Window.orderBy(F.col("gpa").desc(), "id")
w_major_is_international = Window.partitionBy("major", "is_international")
w_major_is_international_by_en = Window.partitionBy("major", "is_international").orderBy(F.col("enrolled_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_major_is_international_by_en_2 = Window.partitionBy("major", "is_international").orderBy(F.col("enrolled_on").desc(), "id")
w_all_by_gpa_ties = Window.orderBy(F.col("gpa").desc())
w_major_is_international_by_en_3 = Window.partitionBy("major", "is_international").orderBy(F.col("enrolled_on").desc(), "id").rowsBetween(-6, -1)
w_all_by_gpa_running = Window.orderBy(F.col("gpa").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_gpa_full = Window.orderBy(F.col("gpa").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_students = (
    df_students
    .filter((F.upper(F.trim(F.col("student_name"))).like("M%")) & (F.col("major").isin("Biz", "Math", "Art")))
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.initcap(F.trim(F.col("student_name"))).alias("student_name_clean"),
    )
)

metrics = (
    recent_students
    .select(
        "id",
        "cohort",
        "enrolled_on",
        "gpa",
        "is_international",
        "student_name_clean",
        F.lag("enrolled_on", 3).over(w_all_by_gpa).alias("prev3_enrolled_on"),
        F.size(F.collect_set("cohort").over(w_major_is_international)).alias("distinct_cohort_in_major_is_international"),
        F.round(F.col("gpa") / F.nullif(F.sum("gpa").over(w_major_is_international), F.lit(0)), 4).alias("gpa_share_of_major_is_international"),
        F.round(F.col("gpa") - F.avg("gpa").over(w_major_is_international), 2).alias("gpa_vs_major_is_international_avg"),
        F.count(F.lit(1)).over(w_major_is_international_by_en).alias("running_count"),
        F.lag("enrolled_on").over(w_major_is_international_by_en_2).alias("prev_enrolled_on"),
        F.dense_rank().over(w_all_by_gpa).alias("gpa_dense_rank"),
        F.row_number().over(w_all_by_gpa).alias("gpa_seq"),
        F.round(F.cume_dist().over(w_all_by_gpa_ties), 3).alias("gpa_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_gpa), 3).alias("gpa_percent_rank"),
        F.max("gpa").over(w_major_is_international_by_en_3).alias("gpa_max_6_prior"),
        F.sum("gpa").over(w_all_by_gpa_running).alias("running_gpa"),
        F.nth_value("enrolled_on", 2).over(w_all_by_gpa_full).alias("second_enrolled_on"),
    )
)

top_rows = metrics.filter(F.col("gpa_dense_rank") <= 3)

result = (
    top_rows
    .select(
        "id",
        "cohort",
        "enrolled_on",
        "gpa",
        "is_international",
        "student_name_clean",
        "prev3_enrolled_on",
        "distinct_cohort_in_major_is_international",
        "gpa_vs_major_is_international_avg",
        "running_count",
        "prev_enrolled_on",
        "gpa_seq",
        "gpa_cume_dist",
        "gpa_percent_rank",
        "gpa_max_6_prior",
        "running_gpa",
    )
)
