from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_score_tier = Window.partitionBy("grade_status", "score_tier")
w_all_by_score = Window.orderBy(F.col("score").asc_nulls_last(), "id")
w_enrolled_at_year_by_enrolled = Window.partitionBy(F.year("enrolled_at")).orderBy("enrolled_at", "id")
w_row_count_student_id_by_tota = Window.partitionBy("row_count", "student_id").orderBy(F.col("total_enrolled_at_decile").desc(), "student_id", "enrolled_at_decile")
w_row_count_student_id_by_tota_2 = Window.partitionBy("row_count", "student_id").orderBy(F.col("total_enrolled_at_decile").desc())
w_student_id_by_total_enrolled = Window.partitionBy("student_id").orderBy("total_enrolled_at_decile")
w_student_id_by_total_enrolled_2 = Window.partitionBy("student_id").orderBy("total_enrolled_at_decile", "student_id", "enrolled_at_decile").rowsBetween(-2, Window.currentRow)
w_row_count = Window.partitionBy("row_count")

prepared = (
    df_enrollments
    .filter((F.col("student_id") >= 9) & (F.col("course_id") <= 6))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 80.2, "A").when(F.col("score") >= 42.4, "B").otherwise("C").alias("score_tier"),
    )
)

with_windows = (
    prepared
    .select(
        "*",
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status_score_tier), F.lit(0)), 4).alias("score_share_of_grade_status_score_tier"),
        F.round(F.cume_dist().over(w_all_by_score), 3).alias("score_cume_dist"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status_score_tier), 2).alias("score_vs_grade_status_score_tier_avg"),
        F.min("score").over(w_grade_status_score_tier).alias("grade_status_score_tier_min_score"),
        F.ntile(10).over(w_enrolled_at_year_by_enrolled).alias("enrolled_at_decile"),
        F.sum("score").over(w_grade_status_score_tier).alias("grade_status_score_tier_sum_score"),
    )
)

totals = (
    with_windows
    .groupBy("student_id", "enrolled_at_decile")
    .agg(
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.sum(F.when(F.col("grade_status") == "PASS", F.col("grade_status_score_tier_sum_score"))).alias("grade_status_score_tier_sum_score_grade_status_p"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.sum("enrolled_at_decile").alias("total_enrolled_at_decile"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("score_cume_dist"), 2).alias("stddev_score_cume_dist"),
    )
)

sequenced = (
    totals
    .select(
        "*",
        F.lag("enrolled_at_decile", 2).over(w_row_count_student_id_by_tota).alias("prev2_enrolled_at_decile"),
        F.round(F.percent_rank().over(w_row_count_student_id_by_tota_2), 3).alias("total_enrolled_at_decile_percent_rank"),
        F.dense_rank().over(w_student_id_by_total_enrolled).alias("total_enrolled_at_decile_dense_rank"),
        F.max("row_count").over(w_student_id_by_total_enrolled_2).alias("row_count_max_3"),
        F.round(F.col("row_count") - F.avg("row_count").over(w_row_count), 2).alias("row_count_vs_row_count_avg"),
        F.count(F.lit(1)).over(w_row_count).alias("rows_in_row_count"),
    )
)

derived = (
    sequenced
    .filter(F.col("enrolled_at_decile") > 4)
    .select(
        "student_id",
        "enrolled_at_decile",
        "row_count",
        "stddev_score_cume_dist",
        "prev2_enrolled_at_decile",
        "total_enrolled_at_decile_percent_rank",
        "total_enrolled_at_decile_dense_rank",
        F.date_trunc("week", F.col("last_enrolled_at")).alias("last_enrolled_at_week"),
        F.size(F.col("grade_status_list")).alias("grade_status_list_count"),
    )
)

result = (
    derived
    .filter(F.col("student_id").isNotNull())
    .select(
        "student_id",
        "enrolled_at_decile",
        "stddev_score_cume_dist",
        "prev2_enrolled_at_decile",
        "total_enrolled_at_decile_percent_rank",
        "total_enrolled_at_decile_dense_rank",
        "last_enrolled_at_week",
        "grade_status_list_count",
        F.when(F.col("grade_status_list_count") >= 2, "HIGH").when(F.col("grade_status_list_count") >= 1, "MEDIUM").otherwise("LOW").alias("grade_status_list_count_tier"),
        (F.col("last_enrolled_at_week") + F.expr("INTERVAL 7 DAYS")).alias("last_enrolled_at_week_plus_7d"),
    )
    .orderBy(F.col("stddev_score_cume_dist").asc(), "grade_status_list_count")
)
