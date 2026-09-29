from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_enrolled_at_dayofweek_by_enr = Window.partitionBy("enrolled_at_dayofweek").orderBy("enrolled_at", "id")
w_all_by_enrolled_at_dayofweek = Window.orderBy(F.col("enrolled_at_dayofweek").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term = Window.partitionBy("term")
w_all_by_enrolled_at_dayofweek_2 = Window.orderBy(F.col("enrolled_at_dayofweek").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_dayofweek_by_enr_2 = Window.partitionBy("enrolled_at_dayofweek").orderBy("enrolled_at", "id").rowsBetween(-2, Window.currentRow)
w_student_id_first_enrolled_at = Window.partitionBy("student_id", "first_enrolled_at_dayofweek").orderBy(F.col("prev_enrolled_at_dayofweek").desc(), "id")
w_term_by_enrolled_at = Window.partitionBy("term").orderBy(F.col("enrolled_at").desc(), "id")
w_student_id_first_enrolled_at_2 = Window.partitionBy("student_id", "first_enrolled_at_dayofweek").orderBy(F.col("prev_enrolled_at_dayofweek").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_enrollments = (
    df_enrollments
    .filter((F.col("score") <= 56.1) & (F.col("course_id") <= 8))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
    )
)

sequenced = (
    prepared_enrollments
    .select(
        "*",
        F.lag("enrolled_at_dayofweek").over(w_all_by_enrolled_at).alias("prev_enrolled_at_dayofweek"),
        F.ntile(4).over(w_enrolled_at_dayofweek_by_enr).alias("enrolled_at_quartile"),
        F.first("enrolled_at_dayofweek").over(w_all_by_enrolled_at_dayofweek).alias("first_enrolled_at_dayofweek"),
        F.size(F.collect_set("grade_status").over(w_term)).alias("distinct_grade_status_in_term"),
        F.sum("enrolled_at_dayofweek").over(w_all_by_enrolled_at_dayofweek_2).alias("running_enrolled_at_dayofweek"),
        F.round(F.avg("enrolled_at_dayofweek").over(w_term), 2).alias("term_avg_enrolled_at_dayofweek"),
        F.round(F.avg("enrolled_at_dayofweek").over(w_enrolled_at_dayofweek_by_enr_2), 2).alias("enrolled_at_dayofweek_avg_3"),
        F.rank().over(w_all_by_enrolled_at).alias("enrolled_at_rank"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.ntile(4).over(w_all_by_enrolled_at).alias("enrolled_at_quartile_2"),
    )
)

leaders = (
    sequenced
    .filter((F.col("enrolled_at_rank") <= 2) & (F.col("first_enrolled_at_dayofweek") > 2))
)

ranked = (
    leaders
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "enrolled_at_quartile",
        "first_enrolled_at_dayofweek",
        "running_enrolled_at_dayofweek",
        "term_avg_enrolled_at_dayofweek",
        "enrolled_at_dayofweek_avg_3",
        "rows_in_term",
        (F.col("rows_in_term") - F.lag("rows_in_term").over(w_student_id_first_enrolled_at)).alias("rows_in_term_change"),
        (F.col("first_enrolled_at_dayofweek") - F.lag("first_enrolled_at_dayofweek").over(w_term_by_enrolled_at)).alias("first_enrolled_at_dayofweek_change"),
        (F.col("rows_in_term") - F.lag("rows_in_term").over(w_term_by_enrolled_at)).alias("rows_in_term_change_2"),
        F.nth_value("first_enrolled_at_dayofweek", 2).over(w_student_id_first_enrolled_at_2).alias("second_first_enrolled_at_dayofweek"),
        F.lag("enrolled_at_dayofweek", 3).over(w_term_by_enrolled_at).alias("prev3_enrolled_at_dayofweek"),
    )
)

stats = (
    ranked
    .filter(F.col("prev3_enrolled_at_dayofweek") > 3)
    .groupBy("second_first_enrolled_at_dayofweek")
    .agg(
        F.sum(F.when(F.col("term").isNotNull(), F.col("rows_in_term_change_2")).otherwise(0)).alias("rows_in_term_change_2_term_is_not_null"),
        F.round(F.sum("second_first_enrolled_at_dayofweek") / F.nullif(F.sum("term_avg_enrolled_at_dayofweek"), F.lit(0)), 3).alias("second_first_enrolled_at_dayofweek_to_term_avg_e"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("first_enrolled_at_dayofweek").alias("max_first_enrolled_at_dayofweek_check"),
    )
    .filter(F.col("max_first_enrolled_at_dayofweek_check") > 3)
    .drop("max_first_enrolled_at_dayofweek_check")
)

result = (
    stats
    .select(
        "second_first_enrolled_at_dayofweek",
        "rows_in_term_change_2_term_is_not_null",
        "second_first_enrolled_at_dayofweek_to_term_avg_e",
        (F.floor(F.col("distinct_ids") / 2) * 2).alias("distinct_ids_bucket"),
        F.abs(F.col("distinct_ids") - 6).alias("distinct_ids_distance"),
    )
    .orderBy(F.col("second_first_enrolled_at_dayofweek_to_term_avg_e").desc())
)
