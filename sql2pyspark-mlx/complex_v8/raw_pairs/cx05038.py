from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_enrolled_at_year_by_enrolled = Window.partitionBy(F.year("enrolled_at")).orderBy("enrolled_at", "id")
w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")
w_enrolled_at_to_date = Window.partitionBy(F.to_date("enrolled_at"))
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_student_id_by_enrolled_at_ti = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc())
w_enrolled_at_year_by_enrolled_2 = Window.partitionBy(F.year("enrolled_at")).orderBy("enrolled_at")
w_distinct_course_ids_row_coun = Window.partitionBy("distinct_course_ids", "row_count").orderBy("distinct_course_ids", "rn", "enrolled_at_rank").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_row_count = Window.partitionBy("row_count")
w_distinct_course_ids_row_coun_2 = Window.partitionBy("distinct_course_ids", "row_count").orderBy("distinct_course_ids")
w_distinct_course_ids_row_coun_3 = Window.partitionBy("distinct_course_ids", "row_count").orderBy("distinct_course_ids", "rn", "enrolled_at_rank").rowsBetween(-3, Window.currentRow)

scoped = (
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

sequenced = (
    scoped
    .select(
        "id",
        "student_id",
        "course_id",
        F.lead("enrolled_at").over(w_all_by_enrolled_at).alias("next_enrolled_at"),
        F.row_number().over(w_enrolled_at_year_by_enrolled).alias("seq_no"),
        F.max("score").over(w_student_id_grade_status).alias("student_id_grade_status_max_score"),
        F.count(F.lit(1)).over(w_enrolled_at_to_date).alias("rows_in_enrolled_at_to_date"),
        F.last("enrolled_at").over(w_grade_status_by_enrolled_at).alias("last_enrolled_at"),
        F.row_number().over(w_student_id_by_enrolled_at).alias("rn"),
        F.rank().over(w_student_id_by_enrolled_at_ti).alias("enrolled_at_rank"),
        F.round(F.cume_dist().over(w_enrolled_at_year_by_enrolled_2), 3).alias("enrolled_at_cume_dist"),
    )
)

top_per_group = sequenced.filter(F.col("enrolled_at_rank") <= 3)

per_group = (
    top_per_group
    .filter(F.col("seq_no") > 2)
    .groupBy("rn", "enrolled_at_rank")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("row_count") >= 2)
)

metrics = (
    per_group
    .select(
        "rn",
        "enrolled_at_rank",
        "row_count",
        "distinct_ids",
        F.min("enrolled_at_rank").over(w_distinct_course_ids_row_coun).alias("min_enrolled_at_rank_so_far"),
        F.dense_rank().over(w_distinct_course_ids_row_coun_2).alias("distinct_course_ids_dense_rank"),
        F.sum("enrolled_at_rank").over(w_distinct_course_ids_row_coun_3).alias("enrolled_at_rank_sum_4"),
        F.round(F.col("row_count") - F.avg("row_count").over(w_row_count), 2).alias("row_count_vs_row_count_avg"),
    )
)

result = (
    metrics
    .filter(F.col("row_count") < 10)
    .select(
        "rn",
        "enrolled_at_rank",
        "row_count",
        "distinct_ids",
        "min_enrolled_at_rank_so_far",
        "enrolled_at_rank_sum_4",
        "row_count_vs_row_count_avg",
        F.coalesce(F.col("distinct_ids"), F.lit(0)).alias("distinct_ids_filled"),
        F.coalesce(F.col("enrolled_at_rank_sum_4"), F.lit(0)).alias("enrolled_at_rank_sum_4_filled"),
    )
    .orderBy("min_enrolled_at_rank_so_far")
)
