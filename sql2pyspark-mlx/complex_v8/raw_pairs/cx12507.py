from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_enrolled_at_mo = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-1, Window.currentRow)
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_term = Window.partitionBy("term")
w_term_student_cohort = Window.partitionBy("term", "student_cohort")
w_student_cohort_student_id_by = Window.partitionBy("student_cohort", "student_id").orderBy("enrolled_at", "id")
w_student_cohort_course_id_by = Window.partitionBy("student_cohort", "course_id").orderBy("score", "id")
w_student_cohort = Window.partitionBy("student_cohort")
w_enrolled_at_to_date_by_score = Window.partitionBy(F.to_date("enrolled_at")).orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_student_cohort_c2023_row_c = Window.partitionBy("n_student_cohort_c2023", "row_count").orderBy(F.col("avg_enrolled_at_quintile").desc(), "enrolled_at_month")
w_row_count = Window.partitionBy("row_count")

recent_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-13 09:00:00").cast("timestamp"))
)

enriched = (
    recent_enrollments.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.cohort").isNotNull()), "inner")
    .filter(F.col("st.major").isin("CS", "Art", "Bio"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        F.col("st.major").alias("student_major"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.max("score").over(w_student_id_by_enrolled_at_mo).alias("score_max_2"),
        F.ntile(5).over(w_student_id_by_enrolled_at).alias("enrolled_at_quintile"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_term), F.lit(0)), 4).alias("score_share_of_term"),
        F.round(F.col("score") - F.avg("score").over(w_term_student_cohort), 2).alias("score_vs_term_student_cohort_avg"),
        F.row_number().over(w_student_cohort_student_id_by).alias("rn"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_student_cohort_course_id_by))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.round(F.col("score") - F.avg("score").over(w_student_cohort), 2).alias("score_vs_student_cohort_avg"),
        F.min("score").over(w_term).alias("term_min_score"),
        F.nth_value("score", 2).over(w_enrolled_at_to_date_by_score).alias("second_score"),
    )
)

rollup_base = (
    windowed
    .filter(F.col("minutes_since_prev_enrolled_at") <= 3500.0)
    .groupBy(F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"))
    .agg(
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.array_sort(F.collect_set("student_cohort")).alias("student_cohort_list"),
        F.sum(F.when(F.col("student_cohort") == "C2023", 1).otherwise(0)).alias("n_student_cohort_c2023"),
        F.round(F.avg("enrolled_at_quintile"), 2).alias("avg_enrolled_at_quintile"),
        F.count("*").alias("row_count"),
        F.round(100 * F.count(F.when(F.col("term") == "2024T2", 1)) / F.count("*"), 1).alias("pct_term_2024t2"),
    )
    .filter(F.col("row_count") >= 2)
)

with_windows = (
    rollup_base
    .select(
        "*",
        F.ntile(3).over(w_n_student_cohort_c2023_row_c).alias("avg_enrolled_at_quintile_tercile"),
        F.round(F.col("avg_enrolled_at_quintile") / F.nullif(F.sum("avg_enrolled_at_quintile").over(w_row_count), F.lit(0)), 4).alias("avg_enrolled_at_quintile_share_of_row_count"),
    )
)

result = (
    with_windows
    .filter(F.col("avg_enrolled_at_quintile_tercile").between(3, 7))
    .select(
        "enrolled_at_month",
        "first_enrolled_at",
        "student_cohort_list",
        "n_student_cohort_c2023",
        "avg_enrolled_at_quintile",
        "row_count",
        "pct_term_2024t2",
        "avg_enrolled_at_quintile_tercile",
        F.round(F.col("row_count") / F.nullif(F.col("avg_enrolled_at_quintile"), F.lit(0)), 3).alias("row_count_per_avg_enrolled_at_quintile"),
        F.month(F.col("enrolled_at_month")).alias("enrolled_at_month_month"),
    )
    .orderBy(F.col("avg_enrolled_at_quintile_tercile").desc())
)
