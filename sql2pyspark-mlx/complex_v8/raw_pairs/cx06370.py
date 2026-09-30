from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .filter(F.col("term") == "2024T2")
    .select("student_id")
)

enrollment_rows_2 = (
    df_enrollments
    .filter(F.col("grade_status").isin("PASS", "INCOMPLETE"))
    .select("student_id")
)

result = enrollment_rows.intersectAll(enrollment_rows_2)
