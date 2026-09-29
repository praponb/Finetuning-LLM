from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("major", "cohort").orderBy("gpa_filled", "id")
w_latest_2 = Window.partitionBy("major", "cohort").orderBy(F.col("gpa_filled").desc(), F.col("id").desc())
w_all_by_enrolled_on_year_dayo = Window.orderBy("enrolled_on_year_dayofweek", "id")
w_major = Window.partitionBy("major")

recent = (
    df_students
    .filter((F.upper(F.trim(F.col("student_name"))).like("B%")) & (~F.col("is_international")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "is_international",
        F.date_trunc("year", F.col("enrolled_on")).alias("enrolled_on_year"),
        F.coalesce(F.col("gpa"), F.lit(0.0)).alias("gpa_filled"),
    )
)

latest = (
    recent
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

enriched = (
    latest
    .select(
        "*",
        F.dayofweek(F.col("enrolled_on_year")).alias("enrolled_on_year_dayofweek"),
        F.lower(F.regexp_replace(F.trim(F.col("student_name")), " ", "_")).alias("student_name_slug"),
    )
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest_per_key = (
    deduped
    .withColumn("rn", F.row_number().over(w_latest_2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_trends = (
    latest_per_key
    .select(
        "id",
        "cohort",
        "is_international",
        "enrolled_on_year",
        "enrolled_on_year_dayofweek",
        "student_name_slug",
        F.dense_rank().over(w_all_by_enrolled_on_year_dayo).alias("enrolled_on_year_dayofweek_dense_rank"),
        F.count(F.lit(1)).over(w_major).alias("rows_in_major"),
        F.size(F.collect_set("cohort").over(w_major)).alias("distinct_cohort_in_major"),
        F.round(F.percent_rank().over(w_all_by_enrolled_on_year_dayo), 3).alias("enrolled_on_year_dayofweek_percent_rank"),
        F.ntile(4).over(w_all_by_enrolled_on_year_dayo).alias("enrolled_on_year_dayofweek_quartile"),
    )
)

exploded_detail_enrollments = (
    with_trends.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.enrolled_on_year_dayofweek",
        "s.rows_in_major",
        "s.distinct_cohort_in_major",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

result = (
    exploded_detail_enrollments
    .select(
        "id",
        "enrolled_on_year_dayofweek",
        "rows_in_major",
        "distinct_cohort_in_major",
        "enrollment_id",
        "enrollment_score",
        "enrollment_course_id",
        "enrollment_enrolled_at",
    )
    .orderBy(F.col("enrollment_enrolled_at").asc_nulls_last())
)
