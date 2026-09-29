from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_level = Window.partitionBy("course_level")
w_term_course_id_by_course_cre = Window.partitionBy("term", "course_id").orderBy(F.col("course_credits").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score = Window.orderBy("score", "id")
w_grade_status_course_id_by_co = Window.partitionBy("grade_status", "course_id").orderBy(F.col("course_credits").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_course_credits_full = Window.orderBy(F.col("course_credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_full = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_course_level_sum_scor = Window.orderBy("course_level_sum_score_minus_last_course_credits", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_level_or_unknown_cour = Window.partitionBy("course_level_or_unknown", "course_credits")
w_all_by_course_level_min_scor = Window.orderBy(F.col("course_level_min_score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_course_level_sum_scor_2 = Window.orderBy("course_level_sum_score_minus_last_course_credits", "id").rowsBetween(-6, -1)
w_score_quartile_by_min_score = Window.partitionBy("score_quartile").orderBy("min_score_so_far", "id")
w_last_course_credits_by_stdde = Window.partitionBy("last_course_credits").orderBy("stddev_rn", "last_course_credits").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_course_credits_by_stdde_2 = Window.partitionBy("last_course_credits").orderBy("stddev_rn", "last_course_credits")

valid = (
    df_enrollments
    .filter(F.col("grade_status") != "PASS")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

joined = (
    valid.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at_label",
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at_label",
        "course_code",
        "course_credits",
        "course_level",
        F.round(F.avg("score").over(w_course_level), 2).alias("course_level_avg_score"),
        F.round(F.avg("course_credits").over(w_course_level), 2).alias("course_level_avg_course_credits"),
        F.min("score").over(w_term_course_id_by_course_cre).alias("min_score_so_far"),
        F.sum("score").over(w_course_level).alias("course_level_sum_score"),
        (F.col("score") - F.lag("score").over(w_all_by_score)).alias("score_change"),
        F.min("score").over(w_course_level).alias("course_level_min_score"),
        F.sum("course_credits").over(w_grade_status_course_id_by_co).alias("running_course_credits"),
        F.nth_value("score", 2).over(w_all_by_course_credits_full).alias("second_score"),
        F.size(F.collect_set("student_id").over(w_course_level)).alias("distinct_student_id_in_course_level"),
        F.ntile(4).over(w_all_by_score).alias("score_quartile"),
        F.last("course_credits").over(w_all_by_score_full).alias("last_course_credits"),
    )
)

annotated = (
    with_windows
    .filter(F.col("running_course_credits") <= 30)
    .select(
        "*",
        F.concat(F.col("course_code"), F.lit("#"), F.col("course_credits").cast("string")).alias("course_code_ref"),
        (F.col("course_level_sum_score") - F.col("last_course_credits")).alias("course_level_sum_score_minus_last_course_credits"),
        F.coalesce(F.col("course_level"), F.lit("UNKNOWN")).alias("course_level_or_unknown"),
    )
)

sequenced = (
    annotated
    .select(
        "*",
        F.nth_value("course_level_sum_score", 2).over(w_all_by_course_level_sum_scor).alias("second_course_level_sum_score"),
        F.count(F.lit(1)).over(w_course_level_or_unknown_cour).alias("rows_in_course_level_or_unknown_course_credits"),
        F.nth_value("score_change", 2).over(w_all_by_course_level_min_scor).alias("second_score_change"),
        F.round(F.avg("course_level_avg_score").over(w_all_by_course_level_sum_scor_2), 2).alias("course_level_avg_score_avg_6_prior"),
        F.row_number().over(w_score_quartile_by_min_score).alias("rn"),
    )
)

stats = (
    sequenced
    .groupBy("last_course_credits")
    .agg(
        F.round(F.stddev("rn"), 2).alias("stddev_rn"),
        F.array_sort(F.collect_set("course_code")).alias("course_code_list"),
        F.max("course_level_sum_score_minus_last_course_credits").alias("max_course_level_sum_score_minus_last_course_cre"),
    )
    .filter(F.col("max_course_level_sum_score_minus_last_course_cre") > 301.2)
    .drop("max_course_level_sum_score_minus_last_course_cre")
)

metrics = (
    stats
    .withColumn("last_known_stddev_rn", F.last("stddev_rn", ignorenulls=True).over(w_last_course_credits_by_stdde))
    .withColumn("last_course_credits_change", F.col("last_course_credits") - F.lag("last_course_credits").over(w_last_course_credits_by_stdde_2))
)

result = (
    metrics
    .filter(F.col("last_course_credits") < 3)
    .select(
        "last_course_credits",
        "stddev_rn",
        "course_code_list",
        "last_known_stddev_rn",
        "last_course_credits_change",
        F.greatest(F.col("last_known_stddev_rn"), F.col("stddev_rn")).alias("greatest_last_known_stddev_rn_stddev_rn"),
        F.when(F.col("stddev_rn") >= 6.5, "large").when(F.col("stddev_rn") >= 3.5, "medium").otherwise("small").alias("stddev_rn_tier"),
    )
    .orderBy(F.col("last_known_stddev_rn").desc_nulls_first())
)
