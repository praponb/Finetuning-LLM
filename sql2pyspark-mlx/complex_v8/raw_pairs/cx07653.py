from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_enrolled_at_ful = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_by_enrolled_at_tie = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc())
w_course_id_by_enrolled_at = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc(), "id")

prepared = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

qualified = (
    prepared.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id") >= 3), "left_anti")
)

ranked = (
    qualified
    .select(
        "*",
        F.last("enrolled_at").over(w_course_id_by_enrolled_at_ful).alias("last_enrolled_at"),
        F.round(F.percent_rank().over(w_course_id_by_enrolled_at_tie), 3).alias("enrolled_at_percent_rank"),
        F.ntile(10).over(w_course_id_by_enrolled_at).alias("enrolled_at_decile"),
        F.first("enrolled_at").over(w_course_id_by_enrolled_at_ful).alias("first_enrolled_at"),
    )
)

result = (
    ranked
    .groupBy("student_id", "first_enrolled_at")
    .pivot("grade_status", ["PASS", "WITHDRAWN", "INCOMPLETE"])
    .agg(F.avg("enrolled_at_decile").alias("avg_v"), F.count("enrolled_at_decile").alias("n"))
)
