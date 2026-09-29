from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_enrolled_at_ful = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_by_enrolled_at = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_course_id_grade_status_by_en = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at", "id")
w_course_id_by_enrolled_at_tie = Window.partitionBy("course_id").orderBy("enrolled_at")
w_course_id_grade_status_by_en_2 = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2024T2")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

sequenced = (
    cleaned_enrollments
    .select(
        "*",
        F.nth_value("enrolled_at", 2).over(w_course_id_by_enrolled_at_ful).alias("second_enrolled_at"),
        F.ntile(10).over(w_course_id_by_enrolled_at).alias("enrolled_at_decile"),
        F.row_number().over(w_all_by_enrolled_at).alias("seq_no"),
        F.lead("enrolled_at").over(w_grade_status_by_enrolled_at).alias("next_enrolled_at"),
        F.ntile(5).over(w_course_id_grade_status_by_en).alias("enrolled_at_quintile"),
        F.dense_rank().over(w_course_id_by_enrolled_at_tie).alias("enrolled_at_dense_rank"),
        F.last("enrolled_at", ignorenulls=True).over(w_course_id_grade_status_by_en_2).alias("last_known_enrolled_at"),
    )
)

summary = (
    sequenced
    .groupBy(
        "seq_no",
        "course_id",
        F.year("second_enrolled_at").alias("second_enrolled_at_year"),
    )
    .agg(
        F.count(F.when(F.col("term") != "2025T2", 1)).alias("n_term_2025t2"),
        F.count(F.when(F.col("term").isNotNull(), 1)).alias("n_term_is_not_null"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
        F.count("*").alias("cnt"),
    )
)

result = (
    summary
    .select(
        "seq_no",
        "second_enrolled_at_year",
        "n_term_2025t2",
        "n_term_is_not_null",
        "distinct_grade_status",
        "cnt",
    )
    .orderBy(F.col("seq_no").desc())
)
