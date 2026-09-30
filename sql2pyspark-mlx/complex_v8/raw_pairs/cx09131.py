from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_term = Window.partitionBy("grade_status", "term")
w_course_id_term_by_score = Window.partitionBy("course_id", "term").orderBy("score", "id")
w_all_by_score_distance_ties = Window.orderBy("score_distance")
w_all_by_score_distance = Window.orderBy("score_distance", "id")
w_course_id_term_by_score_ties = Window.partitionBy("course_id", "term").orderBy("score")

base_enrollments = (
    df_enrollments
    .filter((F.col("term").isin("2024T1", "2024T2")) & (F.col("score") > 17.4))
)

labeled = (
    base_enrollments
    .filter(F.col("course_id").isNotNull())
    .withColumn("score_distance", F.abs(F.col("score") - 38.5))
)

qualified = (
    labeled.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
)

sequenced = (
    qualified
    .select(
        "*",
        F.count(F.lit(1)).over(w_grade_status_term).alias("rows_in_grade_status_term"),
        F.lead("score_distance").over(w_course_id_term_by_score).alias("next_score_distance"),
        F.round(F.col("score_distance") / F.nullif(F.sum("score_distance").over(w_grade_status_term), F.lit(0)), 4).alias("score_distance_share_of_grade_status_term"),
        F.round(F.percent_rank().over(w_all_by_score_distance_ties), 3).alias("score_distance_percent_rank"),
        (F.col("score") - F.lag("score").over(w_all_by_score_distance)).alias("score_change"),
        F.rank().over(w_course_id_term_by_score_ties).alias("score_rank"),
    )
)

result = (
    sequenced
    .groupBy("student_id", "enrolled_at")
    .pivot("grade_status", ["PASS", "WITHDRAWN", "INCOMPLETE"])
    .agg(F.sum("score").alias("amount"), F.max("score").alias("max_v"))
)
