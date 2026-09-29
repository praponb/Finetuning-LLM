from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_grade_status_by_enrolle = Window.partitionBy("term", "grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_grade_status_by_enrolle_2 = Window.partitionBy("term", "grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_score_tier = Window.partitionBy("course_id", "score_tier")
w_student_id_by_score = Window.partitionBy("student_id").orderBy("score", "id")
w_term = Window.partitionBy("term")
w_student_id_by_score_running = Window.partitionBy("student_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_enrolled_at_day = Window.partitionBy("grade_status", "enrolled_at_dayofweek").orderBy(F.col("score").desc_nulls_first(), "id")

recent = (
    df_enrollments
    .filter(F.hour(F.col("enrolled_at")).between(10, 15))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 69.6, "large").when(F.col("score") >= 43.5, "medium").otherwise("small").alias("score_tier"),
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
    )
)

metrics = (
    recent
    .select(
        "*",
        F.first("enrolled_at").over(w_term_grade_status_by_enrolle).alias("first_enrolled_at"),
        F.first("enrolled_at_dayofweek", ignorenulls=True).over(w_term_grade_status_by_enrolle_2).alias("first_known_enrolled_at_dayofweek"),
        F.round(F.avg("enrolled_at_dayofweek").over(w_course_id_score_tier), 2).alias("course_id_score_tier_avg_enrolled_at_dayofweek"),
        F.max("score").over(w_course_id_score_tier).alias("course_id_score_tier_max_score"),
        F.round(F.cume_dist().over(w_student_id_by_score), 3).alias("score_cume_dist"),
        F.round(F.col("enrolled_at_dayofweek") / F.nullif(F.sum("enrolled_at_dayofweek").over(w_term), F.lit(0)), 4).alias("enrolled_at_dayofweek_share_of_term"),
        F.max("enrolled_at_dayofweek").over(w_student_id_by_score_running).alias("max_enrolled_at_dayofweek_so_far"),
        F.round(F.cume_dist().over(w_grade_status_enrolled_at_day), 3).alias("score_cume_dist_2"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
    )
)

result = (
    metrics
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "score_tier",
        "enrolled_at_dayofweek",
        "first_enrolled_at",
        "course_id_score_tier_max_score",
        "score_cume_dist",
        "enrolled_at_dayofweek_share_of_term",
        "max_enrolled_at_dayofweek_so_far",
        "score_cume_dist_2",
        "rows_in_term",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
    .orderBy(F.col("enrolled_at_date").asc_nulls_last())
)
