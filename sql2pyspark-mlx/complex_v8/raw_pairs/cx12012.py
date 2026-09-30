from pyspark.sql import functions as F

cleaned = (
    df_students
    .filter(~F.col("is_international"))
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        F.length(F.trim(F.col("major"))).alias("major_len"),
        F.last_day(F.col("enrolled_on")).alias("enrolled_on_month_end"),
    )
)

filtered = (
    cleaned.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.term") != "2025T2"), "left_anti")
)

per_group = (
    filtered
    .groupBy(
        "major",
        F.date_trunc("month", "enrolled_on").alias("enrolled_on_month"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

labeled = (
    per_group
    .select(
        "*",
        F.date_format(F.col("enrolled_on_month"), "yyyy-MM-dd").alias("enrolled_on_month_label"),
        F.when(F.col("row_count") >= 8, "A").when(F.col("row_count") >= 6, "B").otherwise("C").alias("row_count_tier"),
    )
)

enriched = (
    labeled
    .select(
        "*",
        F.to_date(F.col("enrolled_on_month")).alias("enrolled_on_month_date"),
    )
)

result = (
    enriched
    .filter(F.col("major").isNotNull())
    .select(
        "major",
        "enrolled_on_month",
        "distinct_ids",
        "enrolled_on_month_label",
        "row_count_tier",
        "enrolled_on_month_date",
    )
    .orderBy(F.col("distinct_ids").desc())
)
