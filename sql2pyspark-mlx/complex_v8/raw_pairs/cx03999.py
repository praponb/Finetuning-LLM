from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_ties = Window.orderBy(F.col("score").desc())
w_grade_status_group_enrolled = Window.partitionBy("grade_status_group", F.month("enrolled_at_plus_30m")).orderBy("score_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_group = Window.partitionBy("grade_status_group")

valid = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("PASS", "WITHDRAWN"), "group_a").when(F.col("grade_status") == "INCOMPLETE", "group_b").otherwise("other").alias("grade_status_group"),
        (F.col("enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("enrolled_at_plus_30m"),
    )
)

labeled = valid.select("*", F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"))

metrics = (
    labeled
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "grade_status_group",
        "enrolled_at_plus_30m",
        F.dense_rank().over(w_all_by_score_ties).alias("score_dense_rank"),
        F.first("enrolled_at_plus_30m").over(w_grade_status_group_enrolled).alias("first_enrolled_at_plus_30m"),
        F.size(F.collect_set("student_id").over(w_grade_status_group)).alias("distinct_student_id_in_grade_status_group"),
    )
)

result = (
    metrics
    .select(
        "id",
        "student_id",
        "score",
        "grade_status_group",
        "enrolled_at_plus_30m",
        "score_dense_rank",
        "first_enrolled_at_plus_30m",
        "distinct_student_id_in_grade_status_group",
    )
    .orderBy(F.col("grade_status_group").desc())
)
