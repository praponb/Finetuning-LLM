from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_rows = Window.partitionBy("n_rows")
w_distinct_majors_major_by_gpa = Window.partitionBy("distinct_majors", "major").orderBy("gpa_to_gpa_distance_ratio", "major").rowsBetween(-3, -1)
w_distinct_majors_major_by_gpa_2 = Window.partitionBy("distinct_majors", "major").orderBy("gpa_to_gpa_distance_ratio", "major")
w_major_gpa_to_gpa_distance_ra = Window.partitionBy("major", "gpa_to_gpa_distance_ratio_seq")
w_gpa_to_gpa_distance_ratio_se = Window.partitionBy("gpa_to_gpa_distance_ratio_seq", "distinct_majors").orderBy(F.col("distinct_majors").desc())
w_gpa_to_gpa_distance_ratio_se_2 = Window.partitionBy("gpa_to_gpa_distance_ratio_seq", "distinct_majors").orderBy(F.col("distinct_majors").desc(), "major").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_gpa_to_gpa_distance_ratio_se_3 = Window.partitionBy("gpa_to_gpa_distance_ratio_seq", "distinct_majors").orderBy(F.col("distinct_majors").desc(), "major")

cleaned_students = (
    df_students
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.abs(F.col("gpa") - 2.7).alias("gpa_distance"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_on")).alias("days_since_enrolled_on"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_on")).alias("days_since_enrolled_on_2"),
    )
)

stats = (
    cleaned_students
    .groupBy("major")
    .agg(
        F.min("enrolled_on").alias("first_enrolled_on"),
        F.count("*").alias("n_rows"),
        F.countDistinct("major").alias("distinct_majors"),
        F.round(F.sum("gpa") / F.nullif(F.sum("gpa_distance"), F.lit(0)), 3).alias("gpa_to_gpa_distance_ratio"),
    )
)

without_matches = (
    stats
    .crossJoin(stats.agg(F.avg("distinct_majors").alias("overall_avg_distinct_majors")))
    .filter(F.col("distinct_majors") > F.col("overall_avg_distinct_majors"))
    .drop("overall_avg_distinct_majors")
)

with_trends = (
    without_matches
    .select(
        "major",
        "distinct_majors",
        F.round(F.col("n_rows") - F.avg("n_rows").over(w_n_rows), 2).alias("n_rows_vs_n_rows_avg"),
        F.round(F.avg("distinct_majors").over(w_distinct_majors_major_by_gpa), 2).alias("distinct_majors_avg_3_prior"),
        F.row_number().over(w_distinct_majors_major_by_gpa_2).alias("gpa_to_gpa_distance_ratio_seq"),
    )
)

scored = (
    with_trends
    .withColumn("n_rows_vs_n_rows_avg_filled", F.coalesce(F.col("n_rows_vs_n_rows_avg"), F.lit(0.0)))
)

with_trends_2 = (
    scored
    .select(
        "major",
        "n_rows_vs_n_rows_avg",
        "n_rows_vs_n_rows_avg_filled",
        F.round(F.cume_dist().over(w_gpa_to_gpa_distance_ratio_se), 3).alias("distinct_majors_cume_dist"),
        F.first("n_rows_vs_n_rows_avg_filled").over(w_gpa_to_gpa_distance_ratio_se_2).alias("first_n_rows_vs_n_rows_avg_filled"),
        F.ntile(10).over(w_gpa_to_gpa_distance_ratio_se_3).alias("distinct_majors_decile"),
    )
)

annotated = (
    with_trends_2
    .filter(F.col("first_n_rows_vs_n_rows_avg_filled") >= -8.4)
    .select(
        "major",
        "n_rows_vs_n_rows_avg",
        "n_rows_vs_n_rows_avg_filled",
        "distinct_majors_cume_dist",
        "first_n_rows_vs_n_rows_avg_filled",
        "distinct_majors_decile",
        F.round(F.col("distinct_majors_decile") / F.nullif(F.col("n_rows_vs_n_rows_avg"), F.lit(0)), 3).alias("distinct_majors_decile_per_n_rows_vs_n_rows_avg"),
        F.length(F.trim(F.col("major"))).alias("major_len"),
    )
)

result = (
    annotated
    .filter(F.col("major_len") < 11)
    .select(
        "major",
        "n_rows_vs_n_rows_avg",
        "n_rows_vs_n_rows_avg_filled",
        "distinct_majors_cume_dist",
        "first_n_rows_vs_n_rows_avg_filled",
        "distinct_majors_decile_per_n_rows_vs_n_rows_avg",
        "major_len",
        F.when(F.col("major").isin("Bio", "Math"), "group_a").when(F.col("major") == "CS", "group_b").otherwise("other").alias("major_group"),
    )
    .orderBy(
        F.col("n_rows_vs_n_rows_avg").asc(),
        F.col("major_len").desc_nulls_first(),
    )
)
