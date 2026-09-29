from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_major = Window.partitionBy("major")
w_major_by_gpa_filled = Window.partitionBy("major").orderBy(F.col("gpa_filled").desc(), "id")
w_all_by_gpa_filled_prior_6 = Window.orderBy(F.col("gpa_filled").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_major_gpa_filled_quartile_by = Window.partitionBy("major", "gpa_filled_quartile").orderBy(F.col("avg_gpa_filled").asc_nulls_last(), "gpa_filled_quartile", "major").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_major_gpa_filled_quartile_by_2 = Window.partitionBy("major", "gpa_filled_quartile").orderBy(F.col("avg_gpa_filled").asc_nulls_last(), "gpa_filled_quartile", "major")
w_major_gpa_filled_quartile_by_3 = Window.partitionBy("major", "gpa_filled_quartile").orderBy(F.col("avg_gpa_filled").asc_nulls_last())
w_major_gpa_filled_quartile_by_4 = Window.partitionBy("major", "gpa_filled_quartile").orderBy(F.col("avg_gpa_filled").asc_nulls_last(), "gpa_filled_quartile", "major").rowsBetween(-3, Window.currentRow)
w_major_gpa_filled_quartile_by_5 = Window.partitionBy("major", "gpa_filled_quartile").orderBy(F.col("avg_gpa_filled").asc_nulls_last(), "gpa_filled_quartile", "major").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_students = (
    df_students
    .filter(~F.col("major").isin("CS", "Math"))
    .select(
        "id",
        "student_name",
        "major",
        "gpa",
        F.coalesce(F.col("gpa"), F.lit(0.0)).alias("gpa_filled"),
    )
)

metrics = (
    prepared_students
    .withColumn("gpa_filled_quartile", F.ntile(4).over(w_major_by_gpa_filled))
    .withColumn("gpa_filled_sum_6_prior", F.sum("gpa_filled").over(w_all_by_gpa_filled_prior_6))
)

grouped = (
    metrics
    .groupBy("gpa_filled_quartile", "major")
    .agg(
        F.round(F.avg("gpa_filled"), 2).alias("avg_gpa_filled"),
        F.array_sort(F.collect_set("student_name")).alias("student_name_list"),
    )
)

metrics_2 = (
    grouped
    .select(
        "*",
        F.last("avg_gpa_filled", ignorenulls=True).over(w_major_gpa_filled_quartile_by).alias("last_known_avg_gpa_filled"),
        F.round(F.cume_dist().over(w_major_gpa_filled_quartile_by_2), 3).alias("avg_gpa_filled_cume_dist"),
        F.rank().over(w_major_gpa_filled_quartile_by_3).alias("avg_gpa_filled_rank"),
        F.max("avg_gpa_filled").over(w_major).alias("major_max_avg_gpa_filled"),
        F.round(F.avg("gpa_filled_quartile").over(w_major_gpa_filled_quartile_by_4), 2).alias("gpa_filled_quartile_avg_4"),
        F.nth_value("gpa_filled_quartile", 2).over(w_major_gpa_filled_quartile_by_5).alias("second_gpa_filled_quartile"),
    )
)

without_matches = (
    metrics_2
    .crossJoin(metrics_2.agg(F.min("avg_gpa_filled_rank").alias("overall_min_avg_gpa_filled_rank")))
    .filter(F.col("avg_gpa_filled_rank") > F.col("overall_min_avg_gpa_filled_rank"))
    .drop("overall_min_avg_gpa_filled_rank")
)

per_label = (
    without_matches
    .select(
        "gpa_filled_quartile",
        "major",
        "avg_gpa_filled",
        "last_known_avg_gpa_filled",
        "avg_gpa_filled_cume_dist",
        "avg_gpa_filled_rank",
        "major_max_avg_gpa_filled",
        "gpa_filled_quartile_avg_4",
        "second_gpa_filled_quartile",
        F.explode_outer("student_name_list").alias("student_name_list_item"),
    )
)

result = (
    per_label
    .filter(F.col("avg_gpa_filled_rank") <= 9)
    .select(
        "gpa_filled_quartile",
        "avg_gpa_filled",
        "last_known_avg_gpa_filled",
        "avg_gpa_filled_cume_dist",
        "major_max_avg_gpa_filled",
        "gpa_filled_quartile_avg_4",
        "second_gpa_filled_quartile",
        "student_name_list_item",
        F.round(F.col("avg_gpa_filled") * 1.5, 2).alias("avg_gpa_filled_adj"),
        F.round(F.col("avg_gpa_filled") * 1.07, 2).alias("avg_gpa_filled_adj_2"),
    )
    .orderBy(
        F.col("major_max_avg_gpa_filled").desc(),
        F.col("second_gpa_filled_quartile").desc(),
    )
)
