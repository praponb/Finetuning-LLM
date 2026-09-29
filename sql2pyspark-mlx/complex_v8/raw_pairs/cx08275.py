from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_ties = Window.orderBy("score")
w_all_by_score_full = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

windowed = (
    filtered_enrollments
    .withColumn("score_rank", F.rank().over(w_all_by_score_ties))
    .withColumn("second_enrolled_at", F.nth_value("enrolled_at", 2).over(w_all_by_score_full))
)

result = (
    windowed
    .rollup("student_id", "course_id", "term")
    .agg(
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.countDistinct("term").alias("distinct_terms"),
    )
)
