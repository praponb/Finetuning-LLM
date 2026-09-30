from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_month_student_id = Window.partitionBy(F.month("enrolled_at"), "student_id").orderBy("enrolled_at", "id")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_student_id = Window.partitionBy("student_id")

base_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

with_details = (
    base_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select("s.*", F.col("c.level").alias("course_level"))
)

ranked = (
    with_details
    .select(
        "*",
        F.ntile(4).over(w_enrolled_at_month_student_id).alias("enrolled_at_quartile"),
        F.lag("days_since_enrolled_at", 3).over(w_enrolled_at_month_student_id).alias("prev3_days_since_enrolled_at"),
        F.dense_rank().over(w_all_by_enrolled_at).alias("enrolled_at_dense_rank"),
        F.round(F.avg("days_since_enrolled_at").over(w_student_id), 2).alias("student_id_avg_days_since_enrolled_at"),
        F.min("days_since_enrolled_at").over(w_student_id).alias("student_id_min_days_since_enrolled_at"),
        F.lag("days_since_enrolled_at", 2).over(w_all_by_enrolled_at).alias("prev2_days_since_enrolled_at"),
    )
)

result = (
    ranked
    .cube("term_or_unknown", "enrolled_at_quartile")
    .agg(F.grouping_id().alias("grouping_level"), F.count("*").alias("n_rows"))
    .select(
        F.coalesce(F.col("term_or_unknown"), F.lit("ALL")).alias("term_or_unknown"),
        "enrolled_at_quartile",
        "grouping_level",
        "n_rows",
    )
)
