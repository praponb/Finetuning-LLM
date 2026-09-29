from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("student_id", "term").orderBy(F.col("enrolled_at_quarter").desc(), F.col("id").desc())

filtered = (
    df_enrollments
    .filter((F.to_date(F.col("enrolled_at")) < F.lit("2025-02-06").cast("date")) & (F.col("score") >= 5.9))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

derived = (
    filtered
    .select(
        "*",
        F.upper(F.trim(F.col("term"))).alias("term_clean"),
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

qualified = (
    derived.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
    .filter(F.col("term") == "2025T2")
)

unique_rows = (
    qualified
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .filter(F.length(F.trim(F.col("term_clean"))) >= 4)
    .select(
        "id",
        "student_id",
        "course_id",
        "term_clean",
        "enrolled_at_quarter",
    )
    .orderBy(F.col("id").asc_nulls_last(), "term_clean")
)
