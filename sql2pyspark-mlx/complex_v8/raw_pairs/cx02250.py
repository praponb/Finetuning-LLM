from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_prior_rows_1d_moving = Window.orderBy(F.col("prior_rows_1d").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_prior_rows_1d = Window.orderBy(F.col("prior_rows_1d").desc(), "id")
w_student_id_student_major = Window.partitionBy("student_id", "student_major")
w_student_major_by_prior_rows = Window.partitionBy("student_major").orderBy(F.col("prior_rows_1d").desc())

in_window = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.score").alias("prior_score_1d"),
    )
)

combined_details_student = (
    in_window.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.prior_rows_1d",
        F.col("st.major").alias("student_major"),
    )
)

with_windows = (
    combined_details_student
    .select(
        "id",
        "student_id",
        "prior_rows_1d",
        "student_major",
        F.sum("prior_rows_1d").over(w_all_by_prior_rows_1d_moving).alias("prior_rows_1d_sum_4"),
        F.round(F.cume_dist().over(w_all_by_prior_rows_1d), 3).alias("prior_rows_1d_cume_dist"),
        F.round(F.col("prior_rows_1d") - F.avg("prior_rows_1d").over(w_student_id_student_major), 2).alias("prior_rows_1d_vs_student_id_student_major_avg"),
        F.round(F.cume_dist().over(w_student_major_by_prior_rows), 3).alias("prior_rows_1d_cume_dist_2"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(F.col("term").isin("2025T2", "2024T2"), 1)).alias("enrollment_term_count"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

with_counts = (
    with_windows.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
        "e.first_enrollment_enrolled_at",
    )
)

result = (
    with_counts
    .select("id", "prior_rows_1d_sum_4", "prior_rows_1d")
    .unpivot(
        ids=["id"],
        values=["prior_rows_1d_sum_4", "prior_rows_1d"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "field")
)
