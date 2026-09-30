from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id")
w_student_id_student_cohort_by = Window.partitionBy("student_id", "student_cohort").orderBy(F.col("prior_rows_3d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_student_cohort_by_2 = Window.partitionBy("student_id", "student_cohort").orderBy(F.col("prior_rows_3d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined_ranges = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.score").alias("prior_score_3d"),
    )
)

combined_details = (
    joined_ranges.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.lower(F.col("st.student_name")).like("%ha%")), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.prior_rows_3d",
        "s.prior_score_3d",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

with_parent = (
    combined_details.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.cohort") == "C2023")
    .select("s.*", F.col("st.cohort").alias("student_cohort"), "st.student_name")
)

windowed = (
    with_parent
    .select(
        "*",
        F.round(F.col("prior_rows_3d") - F.avg("prior_rows_3d").over(w_student_id), 2).alias("prior_rows_3d_vs_student_id_avg"),
        F.nth_value("student_enrolled_on", 2).over(w_student_id_student_cohort_by).alias("second_student_enrolled_on"),
        F.last("prior_rows_3d", ignorenulls=True).over(w_student_id_student_cohort_by_2).alias("last_known_prior_rows_3d"),
    )
)

result = (
    windowed
    .select(
        "id",
        "student_id",
        "prior_rows_3d",
        "student_cohort",
        "student_name",
        "prior_rows_3d_vs_student_id_avg",
        "last_known_prior_rows_3d",
    )
)
