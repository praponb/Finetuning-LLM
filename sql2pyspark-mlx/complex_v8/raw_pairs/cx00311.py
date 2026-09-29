from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("major").orderBy(F.col("enrolled_on").desc(), F.col("id").desc())
w_cohort_major_by_days_since_e = Window.partitionBy("cohort", "major").orderBy(F.col("days_since_enrolled_on").desc())
w_cohort_major_by_days_since_e_2 = Window.partitionBy("cohort", "major").orderBy(F.col("days_since_enrolled_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_days_since_enrolled_on_rank = Window.partitionBy("days_since_enrolled_on_rank", "cohort")
w_days_since_enrolled_on_rank_2 = Window.partitionBy("days_since_enrolled_on_rank").orderBy(F.col("days_since_enrolled_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest = Window.partitionBy("cohort").orderBy("running_days_since_enrolled_on_2", "id")

valid = (
    df_students
    .filter((F.col("gpa") > 2.5) & (F.col("cohort") != "C2023"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        F.initcap(F.trim(F.col("cohort"))).alias("cohort_clean"),
    )
)

latest_per_key = (
    valid
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

tagged = (
    latest_per_key
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "cohort_clean",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_on")).alias("days_since_enrolled_on"),
        F.date_format(F.col("enrolled_on"), "yyyy-MM").alias("enrolled_on_label"),
    )
)

with_trends = (
    tagged
    .select(
        "id",
        "cohort",
        "days_since_enrolled_on",
        "enrolled_on_label",
        F.rank().over(w_cohort_major_by_days_since_e).alias("days_since_enrolled_on_rank"),
        F.sum("days_since_enrolled_on").over(w_cohort_major_by_days_since_e_2).alias("running_days_since_enrolled_on"),
    )
)

metrics = (
    with_trends
    .select(
        "id",
        "cohort",
        "days_since_enrolled_on",
        "enrolled_on_label",
        "days_since_enrolled_on_rank",
        "running_days_since_enrolled_on",
        F.round(F.col("days_since_enrolled_on") / F.nullif(F.sum("days_since_enrolled_on").over(w_days_since_enrolled_on_rank), F.lit(0)), 4).alias("days_since_enrolled_on_share_of_days_since_enrol"),
        F.sum("days_since_enrolled_on").over(w_days_since_enrolled_on_rank_2).alias("running_days_since_enrolled_on_2"),
    )
)

latest_per_key_2 = (
    metrics
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key_2
    .groupingSets(
        [["cohort", "days_since_enrolled_on_rank"], ["cohort"], []],
        "cohort", "days_since_enrolled_on_rank",
    )
    .agg(
        F.array_sort(F.collect_set("cohort")).alias("cohort_list"),
        F.percentile_approx("days_since_enrolled_on_share_of_days_since_enrol", 0.5).alias("median_days_since_enrolled_on_share_of_days_sinc"),
        F.countDistinct("cohort").alias("distinct_cohorts"),
    )
    .select(
        F.coalesce(F.col("cohort"), F.lit("ALL")).alias("cohort"),
        "days_since_enrolled_on_rank",
        "cohort_list",
        "median_days_since_enrolled_on_share_of_days_sinc",
        "distinct_cohorts",
    )
    .orderBy(
        F.col("cohort").asc_nulls_last(),
        F.col("days_since_enrolled_on_rank").asc_nulls_last(),
    )
)
