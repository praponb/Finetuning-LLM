from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("student_id").orderBy(F.col("prior_rows_1d").desc(), F.col("id").desc())
w_student_cohort_by_enrolled_a = Window.partitionBy("student_cohort").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_cohort_by_enrolled_a_2 = Window.partitionBy("student_cohort").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prior_rows_1d_student_id_by = Window.partitionBy("prior_rows_1d", "student_id").orderBy(F.col("prior_score_1d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prior_rows_1d_student_id_by_2 = Window.partitionBy("prior_rows_1d", "student_id").orderBy(F.col("prior_score_1d").desc(), "id")
w_enrolled_at_month_student_id = Window.partitionBy(F.month("enrolled_at"), "student_id")

matched = (
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

latest = (
    matched
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

combined_details = (
    latest.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.enrolled_on").isNotNull()), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.prior_rows_1d",
        "s.prior_score_1d",
        F.col("st.cohort").alias("student_cohort"),
        "st.student_name",
        F.col("st.gpa").alias("student_gpa"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.last("prior_rows_1d", ignorenulls=True).over(w_student_cohort_by_enrolled_a).alias("last_known_prior_rows_1d"),
        F.first("enrolled_at").over(w_student_cohort_by_enrolled_a_2).alias("first_enrolled_at"),
        F.nth_value("prior_score_1d", 2).over(w_prior_rows_1d_student_id_by).alias("second_prior_score_1d"),
        F.row_number().over(w_prior_rows_1d_student_id_by_2).alias("rn"),
        F.count(F.lit(1)).over(w_enrolled_at_month_student_id).alias("rows_in_enrolled_at_month_student_id"),
    )
)

result = (
    windowed
    .select(
        "id",
        F.col("rows_in_enrolled_at_month_student_id").alias("m_rows_in_enrolled_at_month_student_id"),
        F.col("prior_rows_1d").alias("prior_rows_1d_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_rows_in_enrolled_at_month_student_id", "prior_rows_1d_v"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
