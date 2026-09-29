from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("major", "cohort_or_unknown").orderBy(F.col("gpa").desc(), F.col("id").desc())
w_dedup = Window.partitionBy("cohort_or_unknown").orderBy("enrolled_on_year", "id")

valid = (
    df_students
    .filter(F.col("gpa") < 3.2)
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        F.date_trunc("week", F.col("enrolled_on")).alias("enrolled_on_week"),
        F.coalesce(F.col("cohort"), F.lit("UNKNOWN")).alias("cohort_or_unknown"),
        F.date_trunc("year", F.col("enrolled_on")).alias("enrolled_on_year"),
    )
)

latest = (
    valid
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

latest_2 = (
    latest
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

cohort_or_unknown_stats = (
    latest_2
    .groupBy("cohort_or_unknown")
    .agg(F.avg("gpa").alias("avg_gpa"))
)

vs_group = (
    latest_2.alias("s")
    .join(cohort_or_unknown_stats.alias("g"), F.col("g.cohort_or_unknown") == F.col("s.cohort_or_unknown"), "inner")
    .filter(F.col("s.gpa") > F.col("g.avg_gpa"))
    .select(
        "s.*",
        "g.avg_gpa",
        F.round(F.col("s.gpa") - F.col("g.avg_gpa"), 2).alias("gpa_minus_avg"),
    )
)

stats = (
    vs_group
    .groupBy(
        F.date_trunc("month", "enrolled_on_year").alias("enrolled_on_year_month"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("avg_gpa") / F.nullif(F.sum("gpa_minus_avg"), F.lit(0)), 3).alias("avg_gpa_to_gpa_minus_avg_ratio"),
        F.sum("avg_gpa").alias("total_avg_gpa"),
        F.count("*").alias("row_count"),
        F.min("gpa_minus_avg").alias("min_gpa_minus_avg"),
    )
)

result = (
    stats
    .select(
        "enrolled_on_year_month",
        "distinct_ids",
        "avg_gpa_to_gpa_minus_avg_ratio",
        "total_avg_gpa",
        "row_count",
        "min_gpa_minus_avg",
    )
    .orderBy(F.col("total_avg_gpa").desc_nulls_first())
)
