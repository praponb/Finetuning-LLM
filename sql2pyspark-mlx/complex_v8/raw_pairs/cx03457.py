from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_international = Window.partitionBy("is_international")
w_all_by_gpa_ties = Window.orderBy("gpa")
w_is_international_major_by_en = Window.partitionBy("is_international", "major").orderBy(F.col("enrolled_on").desc(), "id").rowsBetween(-2, Window.currentRow)

recent = (
    df_students
    .filter((F.col("cohort") != "C2022") & (F.upper(F.trim(F.col("student_name"))).like("D%")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

sequenced = (
    recent
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        F.round(F.avg("gpa").over(w_is_international), 2).alias("is_international_avg_gpa"),
        F.rank().over(w_all_by_gpa_ties).alias("gpa_rank"),
        F.round(F.avg("gpa").over(w_is_international_major_by_en), 2).alias("gpa_avg_3"),
        F.min("gpa").over(w_is_international).alias("is_international_min_gpa"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
    )
)

summarized_rows = (
    sequenced.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.avg_enrollment_score",
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.count("*").alias("enrollment_count_2"))
)

with_counts = (
    summarized_rows.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
    )
)

grouped = (
    with_counts
    .groupBy("major", "enrollment_count_2", "gpa_rank")
    .agg(
        F.count("*").alias("cnt"),
        F.datediff(F.max("enrolled_on"), F.min("enrolled_on")).alias("enrolled_on_span_days"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    grouped
    .select(
        "major",
        "enrollment_count_2",
        "gpa_rank",
        "cnt",
    )
    .orderBy(F.col("cnt").desc())
)
