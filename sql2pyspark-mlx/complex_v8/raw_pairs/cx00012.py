from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("term", "grade_status").orderBy("enrolled_at", "id")

filtered = (
    df_enrollments
    .filter((~F.col("grade_status").isin("FAIL", "WITHDRAWN")) | (~F.col("term").isin("2025T2", "2024T1")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
    )
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

rollup_base = (
    deduped
    .filter(F.col("course_id") <= 11)
    .groupBy("term", "course_id")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
    )
)

result = (
    rollup_base
    .select(
        "term",
        "course_id",
        "row_count",
        "distinct_ids",
        "distinct_grade_status",
        F.when(F.col("term").isin("2025T1", "2025T2"), "group_a").when(F.col("term") == "2024T2", "group_b").otherwise("other").alias("term_group"),
        F.abs(F.col("distinct_grade_status") - 5).alias("distinct_grade_status_distance"),
    )
    .orderBy(F.col("term").desc(), "course_id")
    .limit(20)
)
