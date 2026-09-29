from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")
w_all_by_enrolled_at_running = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isin("INCOMPLETE", "WITHDRAWN")) & (F.col("score") >= 11.3))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

ranked = (
    cleaned_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        F.round(F.avg("score").over(w_student_id_grade_status), 2).alias("student_id_grade_status_avg_score"),
        F.last("enrolled_at", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("last_known_enrolled_at"),
        F.row_number().over(w_all_by_enrolled_at).alias("seq_no"),
    )
)

totals = (
    ranked
    .groupBy("seq_no", "student_id")
    .agg(
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.max("seq_no").alias("max_seq_no"),
        F.round(F.sum("score"), 2).alias("total_score"),
        F.round(F.stddev("score"), 2).alias("stddev_score"),
        F.round(F.avg("student_id_grade_status_avg_score"), 2).alias("avg_student_id_grade_status_avg_score"),
        F.max("student_id_grade_status_avg_score").alias("max_student_id_grade_status_avg_score"),
    )
)

enriched_student = (
    totals.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.major").alias("student_major"),
    )
)

result = (
    enriched_student
    .select(
        "seq_no",
        "student_id",
        "distinct_student_ids",
        "total_score",
        "avg_student_id_grade_status_avg_score",
        "max_student_id_grade_status_avg_score",
        "student_enrolled_on",
        "student_major",
    )
    .orderBy("total_score")
)
