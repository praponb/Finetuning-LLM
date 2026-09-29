from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_term_by_enrolle = Window.partitionBy("grade_status", "term").orderBy("enrolled_at", "id")
w_enrolled_at_to_date = Window.partitionBy(F.to_date("enrolled_at"))
w_enrolled_at_year_grade_statu = Window.partitionBy(F.year("enrolled_at"), "grade_status").orderBy(F.col("enrolled_at_dayofweek").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_year_grade_statu_2 = Window.partitionBy(F.year("enrolled_at"), "grade_status").orderBy(F.col("enrolled_at_dayofweek").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_year_grade_statu_3 = Window.partitionBy(F.year("enrolled_at"), "grade_status").orderBy(F.col("enrolled_at_dayofweek").asc_nulls_last(), "id")
w_all_by_enrolled_at_dayofweek = Window.orderBy("enrolled_at_dayofweek", "id")
w_all_by_enrolled_at_dayofweek_2 = Window.orderBy("enrolled_at_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_dayofweek_3 = Window.orderBy("enrolled_at_dayofweek", "id").rowsBetween(-2, -1)
w_enrolled_at_dayofweek = Window.partitionBy("enrolled_at_dayofweek")
w_last_known_enrolled_at_dayof = Window.partitionBy("last_known_enrolled_at_dayofweek", "enrolled_at_dayofweek")
w_student_id_by_enrolled_at_da = Window.partitionBy("student_id").orderBy("enrolled_at_dayofweek_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_group_second_en = Window.partitionBy("grade_status_group", F.to_date("second_enrolled_at")).orderBy(F.col("min_enrolled_at_dayofweek_so_far").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_enrollments
    .filter((F.col("score") <= 85.2) & (F.col("term") != "2025T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
        F.concat_ws(": ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

metrics = (
    recent
    .select(
        "*",
        F.ntile(5).over(w_grade_status_term_by_enrolle).alias("enrolled_at_quintile"),
        F.count(F.lit(1)).over(w_enrolled_at_to_date).alias("rows_in_enrolled_at_to_date"),
        F.min("enrolled_at_dayofweek").over(w_enrolled_at_year_grade_statu).alias("min_enrolled_at_dayofweek_so_far"),
        F.nth_value("enrolled_at", 2).over(w_enrolled_at_year_grade_statu_2).alias("second_enrolled_at"),
        F.round(F.percent_rank().over(w_enrolled_at_year_grade_statu_3), 3).alias("enrolled_at_dayofweek_percent_rank"),
        F.dense_rank().over(w_all_by_enrolled_at_dayofweek).alias("enrolled_at_dayofweek_dense_rank"),
        F.count(F.lit(1)).over(w_all_by_enrolled_at_dayofweek_2).alias("running_count"),
        F.last("enrolled_at_dayofweek", ignorenulls=True).over(w_all_by_enrolled_at_dayofweek_2).alias("last_known_enrolled_at_dayofweek"),
        F.round(F.avg("enrolled_at_dayofweek").over(w_all_by_enrolled_at_dayofweek_3), 2).alias("enrolled_at_dayofweek_avg_2_prior"),
    )
)

annotated = (
    metrics
    .select(
        "*",
        F.when(F.col("grade_status").isin("WITHDRAWN", "INCOMPLETE"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("running_count").cast("string")).alias("grade_status_ref"),
    )
)

sequenced = (
    annotated
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at_dayofweek",
        "grade_status_term_label",
        "enrolled_at_quintile",
        "min_enrolled_at_dayofweek_so_far",
        "second_enrolled_at",
        "enrolled_at_dayofweek_dense_rank",
        "enrolled_at_dayofweek_avg_2_prior",
        "grade_status_group",
        "grade_status_ref",
        F.count(F.lit(1)).over(w_enrolled_at_dayofweek).alias("rows_in_enrolled_at_dayofweek"),
        F.count(F.lit(1)).over(w_last_known_enrolled_at_dayof).alias("rows_in_last_known_enrolled_at_dayofweek_enrolle"),
        F.sum("enrolled_at_quintile").over(w_student_id_by_enrolled_at_da).alias("running_enrolled_at_quintile"),
        F.nth_value("running_count", 2).over(w_grade_status_group_second_en).alias("second_running_count"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "grade_status_term_label",
        "enrolled_at_quintile",
        "min_enrolled_at_dayofweek_so_far",
        "enrolled_at_dayofweek_dense_rank",
        "grade_status_ref",
        "rows_in_last_known_enrolled_at_dayofweek_enrolle",
        "running_enrolled_at_quintile",
    )
    .orderBy("running_enrolled_at_quintile", F.col("enrolled_at_quintile").desc())
)
