from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_enrolled_at_hour = Window.partitionBy("enrolled_at_hour")
w_all_by_enrolled_at_plus_30m = Window.orderBy(F.col("enrolled_at_plus_30m").desc(), "id")

recent = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-13 09:00:00").cast("timestamp")) & (F.col("grade_status").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("FAIL", "INCOMPLETE"), "group_a").when(F.col("grade_status") == "WITHDRAWN", "group_b").otherwise("other").alias("grade_status_group"),
        F.when(F.col("term").isin("2024T2", "2024T1"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other").alias("term_group"),
    )
)

derived = (
    recent
    .filter(F.col("grade_status").isin("WITHDRAWN", "PASS"))
    .select(
        "id",
        "term",
        "grade_status",
        "enrolled_at",
        "term_group",
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
    )
)

labeled = (
    derived
    .select(
        "*",
        F.coalesce(F.col("enrolled_at_year"), F.lit(0)).alias("enrolled_at_year_filled"),
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
        (F.col("enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("enrolled_at_plus_30m"),
    )
)

sequenced = (
    labeled
    .select(
        "id",
        "term",
        "grade_status",
        "term_group",
        "enrolled_at_year",
        "enrolled_at_plus_30m",
        F.lag("enrolled_at_plus_30m").over(w_grade_status_by_enrolled_at).alias("prev_enrolled_at_plus_30m"),
        F.size(F.collect_set("grade_status").over(w_enrolled_at_hour)).alias("distinct_grade_status_in_enrolled_at_hour"),
        F.ntile(5).over(w_grade_status_by_enrolled_at).alias("enrolled_at_quintile"),
        (F.col("enrolled_at_hour") - F.lag("enrolled_at_hour").over(w_all_by_enrolled_at_plus_30m)).alias("enrolled_at_hour_change"),
    )
)

filtered = (
    sequenced
    .crossJoin(sequenced.agg(F.max("enrolled_at_quintile").alias("overall_max_enrolled_at_quintile")))
    .filter(F.col("enrolled_at_quintile") < F.col("overall_max_enrolled_at_quintile"))
    .drop("overall_max_enrolled_at_quintile")
)

result = (
    filtered
    .select(
        "id",
        "term",
        "distinct_grade_status_in_enrolled_at_hour",
        "enrolled_at_hour_change",
        F.coalesce(F.col("enrolled_at_year"), F.lit(0)).alias("enrolled_at_year_filled"),
    )
)
