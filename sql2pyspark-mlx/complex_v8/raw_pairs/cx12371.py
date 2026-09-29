from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_enrolled_at_plus = Window.partitionBy("student_id", F.month("enrolled_at_plus_2h")).orderBy("enrolled_at")
w_dedup = Window.partitionBy("grade_status_group", "grade_status").orderBy("enrolled_at_rank", "id")

filtered = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        (F.col("enrolled_at") + F.expr("INTERVAL 2 HOURS")).alias("enrolled_at_plus_2h"),
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

screened = (
    filtered.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.credits").between(2, 4)) & (~F.col("c.dept").isin("Biz", "Math")), "left_semi")
)

metrics = (
    screened
    .select(
        "id",
        "grade_status",
        "enrolled_at",
        "term_slug",
        "enrolled_at_plus_2h",
        "enrolled_at_quarter",
        F.rank().over(w_student_id_enrolled_at_plus).alias("enrolled_at_rank"),
    )
)

derived = (
    metrics
    .filter(F.col("enrolled_at_rank") >= 4)
    .select(
        "*",
        F.length(F.trim(F.col("term_slug"))).alias("term_slug_len"),
        F.when(F.col("grade_status").isin("PASS", "WITHDRAWN"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

latest = (
    derived
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

filtered_2 = (
    latest
    .crossJoin(latest.agg(F.min("enrolled_at_rank").alias("overall_min_enrolled_at_rank")))
    .filter(F.col("enrolled_at_rank") > F.col("overall_min_enrolled_at_rank"))
    .drop("overall_min_enrolled_at_rank")
)

scored = (
    filtered_2
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("enrolled_at_plus_2h")) - F.unix_timestamp(F.col("enrolled_at"))) / 60, 1).alias("minutes_enrolled_at_to_enrolled_at_plus_2h"),
        (F.floor(F.col("enrolled_at_rank") / 2) * 2).alias("enrolled_at_rank_bucket"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
    )
)

result = (
    scored
    .select(
        "id",
        "grade_status",
        "enrolled_at",
        "term_slug",
        "enrolled_at_quarter",
        "term_slug_len",
        "grade_status_group",
        "minutes_enrolled_at_to_enrolled_at_plus_2h",
        "enrolled_at_rank_bucket",
        "enrolled_at_label",
    )
    .orderBy(F.col("grade_status_group").desc_nulls_first())
)
