from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at_quarter_p = Window.orderBy("enrolled_at_quarter", "id").rowsBetween(-6, -1)
w_all_by_enrolled_at_quarter = Window.orderBy("enrolled_at_quarter", "id")
w_all_by_enrolled_at_quarter_r = Window.orderBy("enrolled_at_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_enrolled_at_quart = Window.partitionBy("student_id", "enrolled_at_quarter")

recent = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

with_activity = (
    recent.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.cohort") != "C2021") & (F.col("st.enrolled_on").isNotNull()), "left_semi")
)

windowed = (
    with_activity
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "term_prefix",
        "grade_status_or_unknown",
        F.round(F.avg("score").over(w_all_by_enrolled_at_quarter_p), 2).alias("score_avg_6_prior"),
        F.ntile(3).over(w_all_by_enrolled_at_quarter).alias("enrolled_at_quarter_tercile"),
        F.last("score", ignorenulls=True).over(w_all_by_enrolled_at_quarter_r).alias("last_known_score"),
        F.round(F.col("score") - F.avg("score").over(w_student_id_enrolled_at_quart), 2).alias("score_vs_student_id_enrolled_at_quarter_avg"),
        F.count(F.lit(1)).over(w_student_id_enrolled_at_quart).alias("rows_in_student_id_enrolled_at_quarter"),
    )
)

result = (
    windowed
    .groupingSets(
        [["grade_status_or_unknown", "term", "student_id"], ["grade_status_or_unknown"], ["term"], []],
        "grade_status_or_unknown", "term", "student_id",
    )
    .agg(
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.count("*").alias("cnt"),
        F.max("last_known_score").alias("max_last_known_score"),
    )
    .orderBy(
        F.col("grade_status_or_unknown").asc_nulls_last(),
        F.col("term").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
