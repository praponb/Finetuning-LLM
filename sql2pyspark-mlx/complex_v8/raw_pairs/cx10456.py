from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_len_grade_statu = Window.partitionBy("grade_status_len", "grade_status")
w_all_by_score = Window.orderBy(F.col("score").desc_nulls_first(), "id")

recent = (
    df_enrollments
    .filter((F.col("course_id") >= 4) & (F.col("enrolled_at") < F.lit("2025-03-02 08:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

metrics = (
    recent
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "enrolled_at",
        "grade_status_slug",
        "grade_status_len",
        F.count(F.lit(1)).over(w_grade_status_len_grade_statu).alias("rows_in_grade_status_len_grade_status"),
        F.row_number().over(w_all_by_score).alias("score_seq"),
    )
)

derived = (
    metrics
    .select(
        "id",
        "enrolled_at",
        "grade_status_len",
        "rows_in_grade_status_len_grade_status",
        "score_seq",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status_slug")), " ", "_")).alias("grade_status_slug_slug"),
    )
)

top_rows = (
    derived
    .filter(F.col("score_seq") <= 2)
    .drop("score_seq")
)

result = (
    top_rows
    .select(
        "id",
        F.col("grade_status_len").alias("m_grade_status_len"),
        F.col("rows_in_grade_status_len_grade_status").alias("m_rows_in_grade_status_len_grade_status"),
    )
    .unpivot(
        ids=["id"],
        values=["m_grade_status_len", "m_rows_in_grade_status_len_grade_status"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
