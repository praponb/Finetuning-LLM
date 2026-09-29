from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("grade_status", "score_tier").orderBy(F.col("score").desc(), F.col("id").desc())
w_all_by_score_ties = Window.orderBy("score")
w_course_id_by_score_full = Window.partitionBy("course_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_by_score = Window.partitionBy("course_id").orderBy("score", "id")
w_all_by_score_moving_6 = Window.orderBy("score", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_enrollments
    .filter(F.col("course_id") <= 7)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.when(F.col("score") >= 69.6, "A").when(F.col("score") >= 23.7, "B").otherwise("C").alias("score_tier"),
        F.when(F.col("score") >= 63.9, "HIGH").when(F.col("score") >= 38.1, "MEDIUM").otherwise("LOW").alias("score_tier_2"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

latest = (
    valid
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_trends = (
    latest
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_score_ties), 3).alias("score_cume_dist"),
        F.first("score").over(w_course_id_by_score_full).alias("first_score"),
        (F.col("score") - F.lag("score").over(w_course_id_by_score)).alias("score_change"),
        F.round(F.avg("score").over(w_all_by_score_moving_6), 2).alias("score_avg_7"),
    )
)

result = (
    with_trends
    .groupBy("score_tier", "grade_status")
    .pivot("score_tier_2", ["HIGH", "MEDIUM", "LOW"])
    .agg(F.max("first_score"))
    .orderBy("score_tier", "grade_status")
)
