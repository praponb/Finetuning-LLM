from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("student_id").orderBy(F.col("prior_rows_7d").desc(), F.col("id").desc())

in_window = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.score").alias("prior_score_7d"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(F.col("term").isNotNull(), 1)).alias("enrollment_term_count"),
        F.count("*").alias("enrollment_count"),
    )
)

with_counts = (
    in_window.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

latest = (
    with_counts
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

labeled = latest.withColumn("enrolled_at_label", F.date_format(F.col("enrolled_at"), "MMM yyyy"))

result = (
    labeled
    .groupingSets(
        [["enrollment_term_count", "enrollment_count", "student_id"], ["enrollment_term_count"]],
        "enrollment_term_count", "enrollment_count", "student_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.count("*").alias("cnt"),
        F.max("enrollment_term_count").alias("max_enrollment_term_count"),
    )
    .orderBy(
        F.col("enrollment_term_count").asc_nulls_last(),
        F.col("enrollment_count").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
